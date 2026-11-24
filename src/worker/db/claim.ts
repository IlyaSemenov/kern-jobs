// Worker claims identify the active jobs a worker can heartbeat and settle.
// The claim cycle combines batches of jobs without a queue with the heads of free queues.
// Every claim statement must run in its own autocommit transaction: claimQueued takes a transaction-level advisory lock of its queue, which an explicit transaction would hold until its commit.
import { columnsOf, type RecordOf } from "kern-orm"
import { param, queryOn, sql, type SqlFragment, type StatementExecutor } from "kern-orm/db"

import type { JobTable } from "../../sql/job-table"
import { toStatementLabel } from "../../sql/statement"

import { jobTableSql, nowSql } from "./sql"
import { job } from "./tables/job"
import * as t from "./types"
import { isUniqueViolation } from "./violation"

/** Claimed job with its claim id, which every heartbeat and settlement of the claim must match. */
export interface Claim {
  id: string
  claimId: string
  /** Claim lag in seconds: the claim time minus `run_at`. */
  lag: number
  job: ClaimedJob
}

/** Reference to a claim, enough to heartbeat or settle it. */
export type ClaimRef = Pick<Claim, "id" | "claimId">

/** Columns of the claimed row `j` that the worker reads, by its handler, its settlement, and its events, which a claim statement returns and whose record `select` decodes. */
const claimedJobColumns = columnsOf(
  job,
  ["id", "claimId", "task", "scope", "key", "queue", "attempts", "maxAttempts", "payload"],
  { alias: "j" },
)

/** Claimed job: the columns of its row that the worker reads, with SQL NULL of the payload as `undefined`. */
export type ClaimedJob = RecordOf<typeof claimedJobColumns>

/** Returns SQL of the claim lag of the claimed row `j` in seconds at `now`. */
const lagSql = (now: SqlFragment) => sql`extract(epoch from ${now} - j.run_at)::float8`

/** Returns the claim of a job row and its claim lag that a claim statement returns; the statement sets `claim_id` before `RETURNING`, so the row has one. */
function toClaim(row: ClaimedJob, lag: number): Claim {
  return { id: row.id, claimId: row.claimId!, lag, job: row }
}

/** Options of a claim. */
export interface ClaimOptions {
  worker: string
  /** Tasks that the worker implements; it claims no others. */
  tasks: string[]
  /** Lease in seconds per task. */
  leases?: Record<string, number>
  /** Lease in seconds of tasks missing from `leases`. */
  defaultLease?: number
  /** Attempt budget of the failure jobs of each task that has a failure task, which each claim stamps on its job, and clears for a task missing from it. */
  failureMaxAttempts?: Record<string, number>
  /** Time of the statement; `statement_timestamp()` when omitted. */
  now?: Date | null
}

/**
 * Returns SQL of the assignments that claim the row `j` for the worker at `now`: a new claim id, the next attempt, and the lease end, now plus the lease of its task or the default lease.
 * Every claim stamps the attempt budget of the failure job of its task, or clears it for a task without one, so the claim that a terminal failure ends decides whether it inserts a failure job, and a stamp of an earlier claim never counts.
 */
function claimSetSql(table: JobTable, o: ClaimOptions, now: SqlFragment): SqlFragment {
  return sql`state = 'active', claim_id = nextval(${table.sequence}::regclass), locked_by = ${o.worker}, locked_at = ${now},
    attempts = j.attempts + 1,
    lease_until = ${now} + make_interval(secs => coalesce((${param(t.jsonb, o.leases ?? {})} ->> j.task)::float8, ${param(t.doublePrecision, o.defaultLease ?? 60)})),
    failure_max_attempts = (${param(t.jsonb, o.failureMaxAttempts ?? {})} ->> j.task)::int`
}

/** Claims up to `limit` ready jobs without a queue in one statement. */
export async function claimPlain(
  db: StatementExecutor,
  table: JobTable,
  o: ClaimOptions & { limit: number },
): Promise<Claim[]> {
  const now = nowSql(o.now)
  const jobs = jobTableSql(table)
  const rows = await queryOn(db)`
    with c as (
      select id from ${jobs}
      where state = 'pending' and queue is null and run_at <= ${now} and task = any(${param(t.array(t.text), o.tasks)})
        and /*hook:claim_plain*/true
      order by run_at
      limit ${o.limit}
      for update skip locked
    )
    update ${jobs} j set ${claimSetSql(table, o, now)}
    from c where j.id = c.id
    returning ${claimedJobColumns}, ${lagSql(now)} as lag_seconds`
    .comment(toStatementLabel("claim:plain"))
    .select({ job: claimedJobColumns, lag_seconds: t.doublePrecision })
  return rows.map(row => toClaim(row.job, row.lag_seconds))
}

/** Queue of a scope. */
export interface QueueRef {
  scope: string
  queue: string
}

/**
 * Result of claiming a queue head: no free queue, a queue whose head another transaction holds or changed, or a claimed job.
 */
export type QueueClaim =
  | { kind: "none" }
  | { kind: "busy"; queue: QueueRef }
  | { kind: "job"; queue: QueueRef; claim: Claim }

/**
 * Claims the head of one free queue.
 * A loose index scan finds the queue, `pg_try_advisory_xact_lock` serializes claims of it, and the head by `(run_at, id)` is locked with `SKIP LOCKED`: when another transaction holds the head, or moved it after the snapshot, the result is `busy`, and the caller skips the queue until the end of its cycle.
 */
export async function claimQueued(
  db: StatementExecutor,
  table: JobTable,
  o: ClaimOptions & { skip?: QueueRef[]; after?: QueueRef | null },
): Promise<QueueClaim> {
  const skip = o.skip ?? []
  const now = nowSql(o.now)
  const jobs = jobTableSql(table)
  const tasks = param(t.array(t.text), o.tasks)
  const afterScope = param(t.text.nullable(), o.after?.scope ?? null)
  const afterQueue = param(t.text.nullable(), o.after?.queue ?? null)
  const row = await queryOn(db)`
    with recursive qs as (
      -- Loose index scan over distinct (scope, queue) of pending rows: O(queues · log n), independent of backlog size.
      -- Starts after the worker's cursor (round-robin over queues); the caller wraps around on "none".
      (select scope, queue from ${jobs}
       where state = 'pending' and queue is not null
         and (${afterScope} is null or (scope, queue) > (${afterScope}, ${afterQueue}))
       order by scope, queue limit 1)
      union all
      select n.scope, n.queue from qs, lateral (
        select scope, queue from ${jobs}
        where state = 'pending' and queue is not null and (scope, queue) > (qs.scope, qs.queue)
        order by scope, queue limit 1) n
    ), q as (
      -- OFFSET 0 keeps the filters inside the subquery, so the lock is tried only on rows that passed them and LIMIT 1 stops after the first acquired lock under any plan.
      select f.scope, f.queue
      from (
        select c.scope, c.queue
        from qs c
        where /*hook:claim_queue*/true
          and exists (select 1 from ${jobs} h where h.state = 'pending' and h.scope = c.scope and h.queue = c.queue
                      and h.run_at <= ${now} and h.task = any(${tasks}))
          and not exists (select 1 from unnest(${param(
            t.array(t.text),
            skip.map(r => r.scope),
          )}, ${param(
            t.array(t.text),
            skip.map(r => r.queue),
          )}) as s(scope, queue)
                          where s.scope = c.scope and s.queue = c.queue)
          and not exists (select 1 from ${jobs} a where a.state = 'active' and a.scope = c.scope and a.queue = c.queue)
        offset 0
      ) f
      -- Queues whose keys collide share a lock, which only makes one of them busy for a moment.
      where pg_try_advisory_xact_lock(2, hashtext(f.scope || E'\\x1f' || f.queue))
      limit 1
    ), head as (
      -- LATERAL keeps the head an index scan; a plain join let the planner sort the whole queue.
      select h.id from q, lateral (
        select j.id from ${jobs} j
        where j.state = 'pending' and j.scope = q.scope and j.queue = q.queue and j.run_at <= ${now} and j.task = any(${tasks})
        order by j.run_at, j.id
        limit 1) h
    ), locked as (
      -- Only the head is locked, without waiting; the recheck of run_at and of the queue stops a head that a producer moved into the future or into another queue after the snapshot.
      select j.id from ${jobs} j join head on j.id = head.id cross join q
      where j.state = 'pending' and j.run_at <= ${now} and j.scope = q.scope and j.queue = q.queue
      for update of j skip locked
    ), claimed as (
      update ${jobs} j set ${claimSetSql(table, o, now)}
      from locked l where j.id = l.id
      returning ${claimedJobColumns}, ${lagSql(now)} as lag_seconds
    )
    select q.scope as q_scope, q.queue as q_queue, c.* from q left join claimed c on true`
    .comment(toStatementLabel("claim:queued"))
    // A busy queue returns its row without a claimed job, whose columns are all NULL.
    .select({
      q_scope: t.text,
      q_queue: t.text,
      job: claimedJobColumns.nullable(),
      lag_seconds: t.doublePrecision.nullable(),
    })
    .take()
    .optional()
  if (!row) return { kind: "none" }
  const queue = { scope: row.q_scope, queue: row.q_queue }
  if (!row.job) return { kind: "busy", queue }
  return { kind: "job", queue, claim: toClaim(row.job, row.lag_seconds!) }
}

/** Counters of claim cycles. */
export interface ClaimCycleStats {
  uniqueViolations: number
  busy: number
}

/** Claim state of a worker that persists across cycles. */
export interface ClaimState {
  /** Which class gets the larger half of the slots in the next cycle. */
  plainFirst: boolean
  /** Last queue served or skipped; the next queue search starts after it. */
  cursor: QueueRef | null
}

/** Creates the initial claim state of a worker. */
export function createClaimState(): ClaimState {
  return { plainFirst: true, cursor: null }
}

/** Options of a claim cycle. */
export interface ClaimCycleOptions extends ClaimOptions {
  slots: number
  maxRetries?: number
  /**
   * Receives each claim as soon as its statement returns: the claim is committed, and if a later statement of the cycle failed, only this worker could run the job before its lease expires.
   */
  onClaim?: (claim: Claim) => void
  /**
   * Checked before each claim statement after the first; the cycle ends when it returns false, such as when the worker can no longer keep new leases or stops.
   */
  proceed?: () => boolean
}

/**
 * Runs a claim cycle of a worker.
 * Slots are split between jobs without a queue and queued jobs, with the larger half alternating between cycles; a class that has no ready jobs leaves its share to the other.
 * Queues are visited round-robin from the cursor of the worker.
 * A `23505` on the active-queue index, from a stale snapshot, is retried at most `maxRetries` times per cycle.
 */
export async function claimCycle(
  db: StatementExecutor,
  table: JobTable,
  o: ClaimCycleOptions,
  stats: ClaimCycleStats = { uniqueViolations: 0, busy: 0 },
  state: ClaimState = createClaimState(),
): Promise<Claim[]> {
  const result: Claim[] = []
  const take = (claims: Claim[]) => {
    for (const claim of claims) {
      result.push(claim)
      o.onClaim?.(claim)
    }
  }
  let statements = 0
  const proceed = () => statements++ === 0 || (o.proceed?.() ?? true)
  const plainShare = state.plainFirst ? Math.ceil(o.slots / 2) : Math.floor(o.slots / 2)
  state.plainFirst = !state.plainFirst
  if (plainShare > 0 && proceed()) take(await claimPlain(db, table, { ...o, limit: plainShare }))
  const plainExhausted = result.length < plainShare
  const skip: QueueRef[] = []
  let retries = 0
  let wrapped = false
  while (result.length < o.slots) {
    if (!proceed()) return result
    let r: QueueClaim
    try {
      r = await claimQueued(db, table, { ...o, skip, after: state.cursor })
    } catch (e) {
      if (isUniqueViolation(e, "job_active_queue_idx") && retries++ < (o.maxRetries ?? 3)) {
        stats.uniqueViolations++
        continue
      }
      throw e
    }
    if (r.kind === "none") {
      if (state.cursor == null || wrapped) break
      state.cursor = null
      wrapped = true
      continue
    }
    state.cursor = r.queue
    if (r.kind === "busy") {
      stats.busy++
      skip.push(r.queue)
      continue
    }
    take([r.claim])
  }
  if (result.length < o.slots && !plainExhausted && proceed()) {
    take(await claimPlain(db, table, { ...o, limit: o.slots - result.length }))
  }
  return result
}
