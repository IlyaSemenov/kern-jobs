// Leases of claimed jobs: heartbeats that extend them, and the reaper that returns expired jobs or the jobs of a worker that has ended.
import {
  isQueryError,
  param,
  queryOn,
  sql,
  type SqlFragment,
  type StatementExecutor,
} from "kern-orm/db"

import type { JobTable } from "../../sql/job-table"
import { toStatementLabel } from "../../sql/statement"
import type { FailureReason } from "../../task/failure"

import type { ClaimRef } from "./claim"
import { insertFailureJobsSql } from "./failure"
import { jobTableSql, nowSql } from "./sql"
import * as t from "./types"
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
  db: StatementExecutor,
  table: JobTable,
  o: { claims: HeartbeatClaim[]; now?: Date | null },
): Promise<HeartbeatResult> {
  const jobs = jobTableSql(table)
  const ids = param(
    t.array(t.bigintText),
    o.claims.map(c => c.id),
  )
  const claimIds = param(
    t.array(t.bigintText),
    o.claims.map(c => c.claimId),
  )
  const leases = param(
    t.array(t.doublePrecision),
    o.claims.map(c => c.lease),
  )
  const rows = await queryOn(db)`
    with x as (
      select * from unnest(${ids}, ${claimIds}, ${leases}) as x(id, claim_id, lease)
    ), owned as (
      select j.claim_id from ${jobs} j join x on j.id = x.id and j.claim_id = x.claim_id where j.state = 'active'
    ), locked as (
      select j.id, x.lease from ${jobs} j join x on j.id = x.id and j.claim_id = x.claim_id
      where j.state = 'active' and /*hook:heartbeat*/true
      for update of j skip locked
    ), extended as (
      update ${jobs} j set lease_until = greatest(j.lease_until, ${nowSql(o.now)} + make_interval(secs => l.lease))
      from locked l where j.id = l.id
      returning j.claim_id
    )
    select o.claim_id, (o.claim_id in (select claim_id from extended)) as extended from owned o`
    .comment(toStatementLabel("heartbeat:extend"))
    .select({ claim_id: t.bigintText, extended: t.boolean })
  const result: HeartbeatResult = { confirmed: [], busy: [] }
  for (const row of rows) (row.extended ? result.confirmed : result.busy).push(row.claim_id)
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
  /** Condition of the rows of the alias `j`. */
  condition: SqlFragment
  error: string
  reason: Extract<FailureReason, "lease-expired" | "worker-restarted">
}

/** Returns the rows that a reap with these options returns at `now`. */
function toReapTarget(o: ReapOptions, now: SqlFragment): ReapTarget {
  return o.worker === undefined
    ? { condition: sql`j.lease_until < ${now}`, error: "lease expired", reason: "lease-expired" }
    : {
        condition: sql`j.locked_by = ${o.worker}`,
        error: "worker restarted",
        reason: "worker-restarted",
      }
}

/** Options of one reap statement. */
interface ReapStatementOptions {
  limit: number
  /** Id of the only row to return, or `null` for a batch. */
  onlyId: string | null
  /** Detaches each returning row that keeps a key from it. */
  forceDetach: boolean
  lockTimeoutMs: number
}

/**
 * Returns the reap statement of the rows of `target`, which returns its rows as `Reaped`.
 * The lock timeout is set by the statement itself, local to its transaction, so that it holds on any connection of a pool and leaves no setting on it; the join makes the `candidates` select, which precedes the waits of the update, read it first; the locks of the table that the statement takes before it runs are not bounded.
 * Several returning rows of one key are possible: an event during `active` creates a pending row, which is claimed too, and their worker dies.
 * Ranking them in the batch keeps the key on the newest only, so that the statement does not conflict with itself; rows that end as failed are ranked apart, since failed rows are outside key uniqueness.
 * A row that becomes failed with the stamp of a failure job inserts it in the same statement, from the claim id that the update clears, so the batch applies both or neither.
 */
function reapStatement(
  db: StatementExecutor,
  table: JobTable,
  target: ReapTarget,
  now: SqlFragment,
  o: ReapStatementOptions,
) {
  const jobs = jobTableSql(table)
  const onlyId = param(t.bigintText.nullable(), o.onlyId)
  return queryOn(db)`
    with timeout as materialized (
      select set_config('lock_timeout', ${String(o.lockTimeoutMs)}, true)
    ), candidates as (
      select j.id, j.scope, j.task, j.key, j.attempts, j.max_attempts, j.claim_id from ${jobs} j, timeout
      where j.state = 'active' and ${target.condition} and (${onlyId} is null or j.id = ${onlyId})
      order by j.id
      limit ${o.limit}
      for update of j skip locked
    ), ranked as (
      select e.*,
        /*hook:reap*/true as hooked,
        row_number() over (partition by scope, task, key, attempts >= max_attempts order by id desc) as rn,
        exists (select 1 from ${jobs} p
                where p.state = 'pending' and p.scope = e.scope and p.task = e.task and p.key = e.key) as has_pending
      from candidates e
    ), returned as (
      update ${jobs} j set
        state = case when r.attempts >= r.max_attempts then 'failed' else 'pending' end,
        key = case when r.attempts < r.max_attempts and r.key is not null and (${param(t.boolean, o.forceDetach)} or r.has_pending or r.rn > 1)
                   then null else j.key end,
        run_at = ${now}, not_before = ${now}, window_started_at = ${now},
        failed_at = case when r.attempts >= r.max_attempts then ${now} end,
        claim_id = null, locked_by = null, lease_until = null, last_error = ${target.error}
      from ranked r where j.id = r.id
      returning j.*, r.claim_id as failed_claim_id, (j.key is null and r.key is not null) as detached
    ), inserted as (
      ${insertFailureJobsSql(table, sql`returned`, target.reason)}
    )
    select id, state, detached from returned`
    .comment(toStatementLabel("reap:batch"))
    .select({ id: t.bigintText, state: t.text.of<Reaped["state"]>(), detached: t.boolean })
}

/**
 * Returns expired active jobs, or the active jobs of `o.worker`: `failed` on the last attempt, otherwise `pending`.
 * A job that becomes failed with the stamp of a failure job inserts it in the statement that fails it.
 * The reaper never merges: a returning job loses its key when a pending job of the key exists or the batch returns several jobs of the key.
 * The batch runs as one statement, which holds the rows of the whole batch while it waits for the transaction of a producer that inserts a pending job of a returning key; each such wait lasts at most `lockTimeout`.
 * After a `23505` or such a timeout the batch is redone row by row, so a hot or blocked key affects only its own row:
 * a row that still conflicts after `maxRetries` attempts loses its key unconditionally, and a row that times out stays active for the next round.
 */
export async function reap(
  db: StatementExecutor,
  table: JobTable,
  o: ReapOptions = {},
  stats: ReapStats = { uniqueViolations: 0 },
): Promise<Reaped[]> {
  const lockTimeoutMs = Math.max(1, Math.round((o.lockTimeout ?? 1) * 1000))
  const limit = o.limit ?? 100
  const now = nowSql(o.now)
  const target = toReapTarget(o, now)
  const run = async (onlyId: string | null, forceDetach: boolean): Promise<Reaped[]> => {
    await o.pause?.beforeAttempt?.()
    return await reapStatement(db, table, target, now, {
      limit,
      onlyId,
      forceDetach,
      lockTimeoutMs,
    })
  }
  const countLockTimeout = () => (stats.lockTimeouts = (stats.lockTimeouts ?? 0) + 1)
  try {
    return await run(null, false)
  } catch (e) {
    if (isQueryError(e, "lock_not_available")) countLockTimeout()
    else if (isUniqueViolation(e, "job_pending_key_idx")) stats.uniqueViolations++
    else throw e
  }
  const candidates = await queryOn(db)`
    select j.id from ${jobTableSql(table)} j
    where j.state = 'active' and ${target.condition}
    order by j.id
    limit ${limit}`
    .comment(toStatementLabel("reap:ids"))
    .select({ id: t.bigintText })
  const result: Reaped[] = []
  for (const { id } of candidates) {
    for (let attempt = 0; ; attempt++) {
      const force = attempt >= (o.maxRetries ?? 3)
      try {
        const rows = await run(id, force)
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
