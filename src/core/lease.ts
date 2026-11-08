// Leases of claimed jobs: heartbeats that extend them and the reaper that returns expired jobs.
import { isQueryError } from "kern-db"

import { hook, NOW } from "./db"
import type { ClaimRef } from "./job"
import type { JobTable } from "./schema"
import { query, type StatementTarget } from "./statement"
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

// The lock timeout is set by the statement itself, local to its transaction, so that it holds on any connection of a pool and leaves no setting on it; the join makes the `expired` select, which precedes the waits of the update, read it first; the locks of the table that the statement takes before it runs are not bounded.
// Several expired rows of one key are possible: an event during `active` creates a pending row, which is claimed too, and their worker dies.
// Ranking them in the batch keeps the key on the newest only, so that the statement does not conflict with itself; rows that end as failed are ranked apart, since failed rows are outside key uniqueness.
const reapSql = (table: JobTable) => `with timeout as materialized (
    select set_config('lock_timeout', $5::text, true)
  ), expired as (
    select j.id, j.scope, j.task, j.key, j.attempts, j.max_attempts from ${table.name} j, timeout
    where j.state = 'active' and j.lease_until < ${NOW} and ($3::text::bigint is null or j.id = $3::text::bigint)
    order by j.id
    limit $2::text::int
    for update of j skip locked
  ), ranked as (
    select e.*,
      ${hook("reap")} as hooked,
      row_number() over (partition by scope, task, key, attempts >= max_attempts order by id desc) as rn,
      exists (select 1 from ${table.name} p
              where p.state = 'pending' and p.scope = e.scope and p.task = e.task and p.key = e.key) as has_pending
    from expired e
  )
  update ${table.name} j set
    state = case when r.attempts >= r.max_attempts then 'failed' else 'pending' end,
    key = case when r.attempts < r.max_attempts and r.key is not null and ($4::text::boolean or r.has_pending or r.rn > 1)
               then null else j.key end,
    run_at = ${NOW}, not_before = ${NOW}, window_started_at = ${NOW},
    failed_at = case when r.attempts >= r.max_attempts then ${NOW} end,
    claim_id = null, locked_by = null, lease_until = null, last_error = 'lease expired'
  from ranked r where j.id = r.id
  returning j.id, j.state, (j.key is null and r.key is not null)::text as detached`

/**
 * Returns expired active jobs: `failed` on the last attempt, otherwise `pending`.
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
  const run = async (onlyId: string | null, forceDetach: boolean): Promise<Reaped[]> => {
    await o.pause?.beforeAttempt?.()
    const rows = await query(db, reapSql(table), [
      o.now ?? null,
      o.limit ?? 100,
      onlyId,
      forceDetach,
      lockTimeoutMs,
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
  const expired = await query(
    db,
    `select id from ${table.name} where state = 'active' and lease_until < ${NOW} order by id limit $2::text::int`,
    [o.now ?? null, o.limit ?? 100],
  )
  const result: Reaped[] = []
  for (const { id } of expired) {
    for (let attempt = 0; ; attempt++) {
      const force = attempt >= (o.maxRetries ?? 3)
      try {
        const rows = await run(id!, force)
        result.push(...rows)
        if (force)
          stats.forcedDetaches = (stats.forcedDetaches ?? 0) + rows.filter(r => r.detached).length
        break
      } catch (e) {
        // The row waits for a producer: leave it expired and active, so that the next round returns it once the producer ends.
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
