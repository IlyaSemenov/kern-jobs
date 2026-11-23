// Leases of claimed jobs: heartbeats that extend them, and the reaper that returns expired jobs or the jobs of a worker that has ended.
import { isQueryError } from "kern-orm/db"

import type { JobTable } from "../../sql/job-table"
import { hook, NOW } from "../../sql/postgres"
import { labelSql, type Param } from "../../sql/statement"
import type { FailureReason } from "../../task/failure"

import type { ClaimRef } from "./claim"
import { insertFailureJobsSql } from "./failure"
import { query, type StatementTarget } from "./query"
import { isUniqueViolation } from "./violation"

/** Claim that a heartbeat extends, with its lease in seconds. */
export interface HeartbeatClaim extends ClaimRef {
  lease: number
}

/** Result of a heartbeat by claim ids; a claim in neither list is lost. */
export interface HeartbeatResult {
  /** Claims whose leases the heartbeat extended. */
  confirmed: string[]
  /**
   * Claims valid in the snapshot of the heartbeat whose rows another transaction held or changed, such as the fail transaction of the claim: the heartbeat neither extended nor lost them.
   */
  busy: string[]
}

/**
 * Extends the lease of each claim to now plus its own lease and never shortens it.
 * Rows that another transaction holds are skipped without waiting: a fail transaction holds the row of its claim while it waits for the pending job of its key, which a producer can hold for as long as its transaction lasts, and waiting for it would stop the heartbeat of every other claim of the worker.
 */
export async function heartbeat(
  db: StatementTarget,
  table: JobTable,
  o: { claims: HeartbeatClaim[]; now?: Date | null },
): Promise<HeartbeatResult> {
  const rows = await query(
    db,
    labelSql(
      "heartbeat:extend",
      `with x as (
       select * from unnest($2::text::bigint[], $3::text::bigint[], $4::text::float8[]) as x(id, claim_id, lease)
     ), owned as (
       select j.claim_id from ${table.name} j join x on j.id = x.id and j.claim_id = x.claim_id where j.state = 'active'
     ), locked as (
       select j.id, x.lease from ${table.name} j join x on j.id = x.id and j.claim_id = x.claim_id
       where j.state = 'active' and ${hook("heartbeat")}
       for update of j skip locked
     ), extended as (
       update ${table.name} j set lease_until = greatest(j.lease_until, ${NOW} + make_interval(secs => l.lease))
       from locked l where j.id = l.id
       returning j.claim_id
     )
     select o.claim_id, (o.claim_id in (select claim_id from extended))::text as extended from owned o`,
    ),
    [
      o.now ?? null,
      o.claims.map(c => c.id),
      o.claims.map(c => c.claimId),
      o.claims.map(c => String(c.lease)),
    ],
  )
  const result: HeartbeatResult = { confirmed: [], busy: [] }
  for (const row of rows)
    (row.extended === "true" ? result.confirmed : result.busy).push(row.claim_id!)
  return result
}

/** Job returned by the reaper. */
export interface Reaped {
  id: string
  state: "pending" | "failed"
  /** The job lost its key. */
  detached: boolean
}

/** Options of the reaper. */
export interface ReapOptions {
  /** Time of the statements; `statement_timestamp()` when omitted. */
  now?: Date | null
  /**
   * Worker whose active jobs to return whatever their leases, with `last_error` `worker restarted`, such as at the start of a worker of the same id once the session of the previous one has ended; the jobs of expired leases when omitted.
   */
  worker?: string | undefined
  limit?: number
  /** Attempts per row in the per-row fallback before the row is detached unconditionally. */
  maxRetries?: number
  /** Seconds that each lock wait of a statement lasts before the statement gives up, such as for the transaction of a producer: positive, since PostgreSQL reads 0 as no limit; 1 when omitted. */
  lockTimeout?: number
  /** Breakpoint before every statement attempt for deterministic tests. */
  pause?: { beforeAttempt?: () => Promise<void> }
}

/** Counters of a reap. */
export interface ReapStats {
  /** Statements that failed with `23505` and were redone. */
  uniqueViolations: number
  /** Statements that gave up a lock wait, such as for the transaction of a producer. */
  lockTimeouts?: number
  /** Rows that lost their key unconditionally after `maxRetries` conflicts. */
  forcedDetaches?: number
}

/** Active rows that a reap returns, the `last_error` that it gives them, and the reason that the `JobFailure` of a failure job records. */
interface ReapTarget {
  /** Condition of the rows of the alias `j` in the reap statement, whose `$1` is its time and `$6` the worker. */
  condition: string
  /** Parameters of the reap statement after its first five: the worker, if any. */
  params: string[]
  error: string
  reason: Extract<FailureReason, "lease-expired" | "worker-restarted">
  /** Statement of the ids of the rows, in the order of their ids, up to the limit. */
  ids: { text: string; params: Param[] }
}

/** Returns the condition of the rows that a reap with these options returns, the parameters of its statement, and the statement of their ids. */
function toReapTarget(table: JobTable, o: ReapOptions): ReapTarget {
  const limit = o.limit ?? 100
  // Each statement of ids names only its own parameters, since PostgreSQL cannot type a parameter that a statement does not use.
  return o.worker === undefined
    ? {
        condition: `j.lease_until < ${NOW}`,
        params: [],
        error: "lease expired",
        reason: "lease-expired",
        ids: {
          text: labelSql(
            "reap:ids",
            `select id from ${table.name} where state = 'active' and lease_until < ${NOW} order by id limit $2::text::int`,
          ),
          params: [o.now ?? null, limit],
        },
      }
    : {
        condition: "j.locked_by = $6::text",
        params: [o.worker],
        error: "worker restarted",
        reason: "worker-restarted",
        ids: {
          text: labelSql(
            "reap:ids",
            `select id from ${table.name} where state = 'active' and locked_by = $2::text order by id limit $1::text::int`,
          ),
          params: [limit, o.worker],
        },
      }
}

// The lock timeout is set by the statement itself, local to its transaction, so that it holds on any connection of a pool and leaves no setting on it; the join makes the `candidates` select, which precedes the waits of the update, read it first; the locks of the table that the statement takes before it runs are not bounded.
// Several returning rows of one key are possible: an event during `active` creates a pending row, which is claimed too, and their worker dies.
// Ranking them in the batch keeps the key on the newest only, so that the statement does not conflict with itself; rows that end as failed are ranked apart, since failed rows are outside key uniqueness.
// A row that becomes failed with the stamp of a failure job inserts it in the same statement, from the claim id that the update clears, so the batch applies both or neither.
const reapSql = (table: JobTable, target: ReapTarget) =>
  labelSql(
    "reap:batch",
    `with timeout as materialized (
    select set_config('lock_timeout', $5::text, true)
  ), candidates as (
    select j.id, j.scope, j.task, j.key, j.attempts, j.max_attempts, j.claim_id from ${table.name} j, timeout
    where j.state = 'active' and ${target.condition} and ($3::text::bigint is null or j.id = $3::text::bigint)
    order by j.id
    limit $2::text::int
    for update of j skip locked
  ), ranked as (
    select e.*,
      ${hook("reap")} as hooked,
      row_number() over (partition by scope, task, key, attempts >= max_attempts order by id desc) as rn,
      exists (select 1 from ${table.name} p
              where p.state = 'pending' and p.scope = e.scope and p.task = e.task and p.key = e.key) as has_pending
    from candidates e
  ), returned as (
    update ${table.name} j set
      state = case when r.attempts >= r.max_attempts then 'failed' else 'pending' end,
      key = case when r.attempts < r.max_attempts and r.key is not null and ($4::text::boolean or r.has_pending or r.rn > 1)
                 then null else j.key end,
      run_at = ${NOW}, not_before = ${NOW}, window_started_at = ${NOW},
      failed_at = case when r.attempts >= r.max_attempts then ${NOW} end,
      claim_id = null, locked_by = null, lease_until = null, last_error = '${target.error}'
    from ranked r where j.id = r.id
    returning j.*, r.claim_id as failed_claim_id, (j.key is null and r.key is not null)::text as detached
  ), inserted as (
    ${insertFailureJobsSql(table, "returned", `'${target.reason}'`)}
  )
  select id, state, detached from returned`,
  )

/**
 * Returns expired active jobs, or the active jobs of `o.worker`: `failed` on the last attempt, otherwise `pending`.
 * A job that becomes failed with the stamp of a failure job inserts it in the statement that fails it.
 * The reaper never merges: a returning job loses its key when a pending job of the key exists or the batch returns several jobs of the key.
 * The batch runs as one statement, which holds the rows of the whole batch while it waits for the transaction of a producer that inserts a pending job of a returning key; each such wait lasts at most `lockTimeout`.
 * After a `23505` or such a timeout the batch is redone row by row, so a hot or blocked key affects only its own row:
 * a row that still conflicts after `maxRetries` attempts loses its key unconditionally, and a row that times out stays active for the next round.
 */
export async function reap(
  db: StatementTarget,
  table: JobTable,
  o: ReapOptions = {},
  stats: ReapStats = { uniqueViolations: 0 },
): Promise<Reaped[]> {
  const lockTimeoutMs = Math.max(1, Math.round((o.lockTimeout ?? 1) * 1000))
  const target = toReapTarget(table, o)
  const run = async (onlyId: string | null, forceDetach: boolean): Promise<Reaped[]> => {
    await o.pause?.beforeAttempt?.()
    const rows = await query(db, reapSql(table, target), [
      o.now ?? null,
      o.limit ?? 100,
      onlyId,
      forceDetach,
      lockTimeoutMs,
      ...target.params,
    ])
    return rows.map(r => ({
      id: r.id!,
      state: r.state as Reaped["state"],
      detached: r.detached === "true",
    }))
  }
  const countLockTimeout = () => (stats.lockTimeouts = (stats.lockTimeouts ?? 0) + 1)
  try {
    return await run(null, false)
  } catch (e) {
    if (isQueryError(e, "lock_not_available")) countLockTimeout()
    else if (isUniqueViolation(e, "job_pending_key_idx")) stats.uniqueViolations++
    else throw e
  }
  const candidates = await query(db, target.ids.text, target.ids.params)
  const result: Reaped[] = []
  for (const { id } of candidates) {
    for (let attempt = 0; ; attempt++) {
      const force = attempt >= (o.maxRetries ?? 3)
      try {
        const rows = await run(id!, force)
        result.push(...rows)
        if (force)
          stats.forcedDetaches = (stats.forcedDetaches ?? 0) + rows.filter(r => r.detached).length
        break
      } catch (e) {
        // The row waits for a producer: leave it active, so that the next round returns it once the producer ends.
        if (isQueryError(e, "lock_not_available")) {
          countLockTimeout()
          break
        }
        if (!isUniqueViolation(e, "job_pending_key_idx") || force) throw e
        stats.uniqueViolations++
      }
    }
  }
  return result
}
