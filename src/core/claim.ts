// Claiming: batches of jobs without a queue, heads of free queues, and the claim cycle that combines them.
// Every claim statement must run in its own autocommit transaction: claimQueued takes a transaction-level advisory lock of its queue, which an explicit transaction would hold until its commit.
import { hook, NOW } from "./db"
import { type Claim, jobColumns, toClaim } from "./job"
import type { JobTable } from "./schema"
import { query, type StatementTarget } from "./statement"
import { isUniqueViolation } from "./violation"

/** Options of a claim. */
export interface ClaimOptions {
  worker: string
  /** Tasks that the worker implements; it claims no others. */
  tasks: string[]
  /** Lease in seconds per task. */
  leases?: Record<string, number>
  /** Lease in seconds of tasks missing from `leases`. */
  defaultLease?: number
  /** Time of the statement; `statement_timestamp()` when omitted. */
  now?: Date | null
}

/** SQL of the lease end of the claimed row `j`: now plus the lease of its task from `$4`, or the default `$5`. */
const LEASE = `${NOW} + make_interval(secs => coalesce(($4::text::jsonb ->> j.task)::float8, $5::text::float8))`

/** SQL of the next claim id of a job table. */
const nextClaimId = (table: JobTable) => `nextval('${table.sequence.replace(/'/g, "''")}')`

/** Claims up to `limit` ready jobs without a queue in one statement. */
export async function claimPlain(
  db: StatementTarget,
  table: JobTable,
  o: ClaimOptions & { limit: number },
): Promise<Claim[]> {
  const rows = await query(
    db,
    `with c as (
       select id from ${table.name}
       where state = 'pending' and queue is null and run_at <= ${NOW} and task = any($2::text::text[])
         and ${hook("claim_plain")}
       order by run_at
       limit $3::text::int
       for update skip locked
     )
     update ${table.name} j set state = 'active', claim_id = ${nextClaimId(table)}, locked_by = $6::text, locked_at = ${NOW},
       attempts = j.attempts + 1, lease_until = ${LEASE}
     from c where j.id = c.id
     returning ${jobColumns("j")}, extract(epoch from ${NOW} - j.run_at)::float8 as lag_seconds`,
    [
      o.now ?? null,
      o.tasks,
      o.limit,
      JSON.stringify(o.leases ?? {}),
      o.defaultLease ?? 60,
      o.worker,
    ],
  )
  return rows.map(toClaim)
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
  db: StatementTarget,
  table: JobTable,
  o: ClaimOptions & { skip?: QueueRef[]; after?: QueueRef | null },
): Promise<QueueClaim> {
  const skip = o.skip ?? []
  const rows = await query(
    db,
    `with recursive qs as (
       -- Loose index scan over distinct (scope, queue) of pending rows: O(queues · log n), independent of backlog size.
       -- Starts after the worker's cursor (round-robin over queues); the caller wraps around on "none".
       (select scope, queue from ${table.name}
        where state = 'pending' and queue is not null
          and ($7::text is null or (scope, queue) > ($7::text, $8::text))
        order by scope, queue limit 1)
       union all
       select n.scope, n.queue from qs, lateral (
         select scope, queue from ${table.name}
         where state = 'pending' and queue is not null and (scope, queue) > (qs.scope, qs.queue)
         order by scope, queue limit 1) n
     ), q as (
       -- OFFSET 0 keeps the filters inside the subquery, so the lock is tried only on rows that passed them and LIMIT 1 stops after the first acquired lock under any plan.
       select f.scope, f.queue
       from (
         select c.scope, c.queue
         from qs c
         where ${hook("claim_queue")}
           and exists (select 1 from ${table.name} h where h.state = 'pending' and h.scope = c.scope and h.queue = c.queue
                       and h.run_at <= ${NOW} and h.task = any($2::text::text[]))
           and not exists (select 1 from unnest($3::text::text[], $9::text::text[]) as s(scope, queue)
                           where s.scope = c.scope and s.queue = c.queue)
           and not exists (select 1 from ${table.name} a where a.state = 'active' and a.scope = c.scope and a.queue = c.queue)
         offset 0
       ) f
       -- Queues whose keys collide share a lock, which only makes one of them busy for a moment.
       where pg_try_advisory_xact_lock(2, hashtext(f.scope || E'\\x1f' || f.queue))
       limit 1
     ), head as (
       -- LATERAL keeps the head an index scan; a plain join let the planner sort the whole queue.
       select h.id from q, lateral (
         select j.id from ${table.name} j
         where j.state = 'pending' and j.scope = q.scope and j.queue = q.queue and j.run_at <= ${NOW} and j.task = any($2::text::text[])
         order by j.run_at, j.id
         limit 1) h
     ), locked as (
       -- Only the head is locked, without waiting; the recheck of run_at and of the queue stops a head that a producer moved into the future or into another queue after the snapshot.
       select j.id from ${table.name} j join head on j.id = head.id cross join q
       where j.state = 'pending' and j.run_at <= ${NOW} and j.scope = q.scope and j.queue = q.queue
       for update of j skip locked
     ), claimed as (
       update ${table.name} j set state = 'active', claim_id = ${nextClaimId(table)}, locked_by = $6::text, locked_at = ${NOW},
         attempts = j.attempts + 1, lease_until = ${LEASE}
       from locked l where j.id = l.id
       returning ${jobColumns("j")}, extract(epoch from ${NOW} - j.run_at)::float8 as lag_seconds
     )
     select q.scope as q_scope, q.queue as q_queue, c.* from q left join claimed c on true`,
    [
      o.now ?? null,
      o.tasks,
      skip.map(r => r.scope),
      JSON.stringify(o.leases ?? {}),
      o.defaultLease ?? 60,
      o.worker,
      o.after?.scope ?? null,
      o.after?.queue ?? null,
      skip.map(r => r.queue),
    ],
  )
  if (rows.length === 0) return { kind: "none" }
  const row = rows[0]!
  const queue = { scope: row.q_scope!, queue: row.q_queue! }
  if (row.id == null) return { kind: "busy", queue }
  return { kind: "job", queue, claim: toClaim(row) }
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
  db: StatementTarget,
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
