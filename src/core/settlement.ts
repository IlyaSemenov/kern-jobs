// Settlement of a claimed job: complete, fail with retry, merge, or a final failure, and requeue of a job whose attempt is given back.
import type { Database } from "kern-db"

import { NOW } from "./db"
import type { ClaimRef } from "./job"
import type { JobTable } from "./schema"
import { query, type StatementTarget } from "./statement"
import { isUniqueViolation } from "./violation"

/** Completes a job by deleting its active row; `false` means the claim was lost. */
export async function complete(
  db: StatementTarget,
  table: JobTable,
  c: ClaimRef,
): Promise<boolean> {
  const rows = await query(
    db,
    `delete from ${table.name} where id = $1::text::bigint and claim_id = $2::text::bigint and state = 'active' returning id`,
    [c.id, c.claimId],
  )
  return rows.length === 1
}

/** Outcome of a fail. */
export type FailOutcome = "fenced" | "failed" | "retry" | "superseded" | "replaced-pending"

/** Options of a fail. */
export interface FailOptions {
  error: string
  /** Fails the job for good, without retries. */
  terminal?: boolean
  /** Delay of the retry in seconds. */
  backoff: number
  /** Time of the statements; `statement_timestamp()` when omitted. */
  now?: Date | null
  /** Retries of the transaction after a `23505` on the pending-key index. */
  maxRetries?: number
  /**
   * Breakpoints between the statements of the transaction for deterministic tests; they get the database, whose statements in them run in the transaction.
   */
  pause?: {
    afterOwnLock?: (tx: StatementTarget) => Promise<void>
    afterCompetitorCheck?: (tx: StatementTarget) => Promise<void>
    beforeCommit?: (tx: StatementTarget) => Promise<void>
  }
}

/**
 * Settles an active job after an error: `failed` on a terminal error or the last attempt, otherwise back to pending with a backoff, or merged with the pending job of its key by the key mode of that job.
 * Runs one `READ COMMITTED` transaction of the database, which reserves a connection for it, so statements of other handlers never run inside it.
 * A lost connection throws `TransactionConnectionError` and is never retried on.
 */
export async function fail(
  database: Pick<Database, "execute" | "transaction">,
  table: JobTable,
  c: ClaimRef,
  o: FailOptions,
  stats: { uniqueViolations: number } = { uniqueViolations: 0 },
): Promise<FailOutcome> {
  return await returnClaim(database, table, c, { ...o, refund: false }, stats)
}

/** Outcome of a requeue. */
export type RequeueOutcome = "fenced" | "requeued" | "superseded" | "replaced-pending"

/** Options of a requeue: those of fail that do not decide the outcome. */
export type RequeueOptions = Pick<FailOptions, "now" | "maxRetries" | "pause">

/**
 * Returns an active job whose handler was interrupted or never started to pending at once, giving its claimed attempt back and keeping its `last_error`, or merges it with the pending job of its key by the key mode of that job, as fail does.
 * It never fails a job for good, also on its last attempt, since the attempt does not count.
 * Runs the transaction of fail, with its retries after a `23505`.
 */
export async function requeue(
  database: Pick<Database, "execute" | "transaction">,
  table: JobTable,
  c: ClaimRef,
  o: RequeueOptions = {},
  stats: { uniqueViolations: number } = { uniqueViolations: 0 },
): Promise<RequeueOutcome> {
  const outcome = await returnClaim(
    database,
    table,
    c,
    { ...o, error: null, backoff: 0, refund: true },
    stats,
  )
  // A refunded attempt never makes the job failed.
  return outcome === "retry" ? "requeued" : (outcome as Exclude<FailOutcome, "failed" | "retry">)
}

/** Options of the transaction that returns a claim: those of fail, `error: null` to keep `last_error`, and `refund` to give the attempt back. */
interface ReturnOptions extends Omit<FailOptions, "error"> {
  error: string | null
  /** Gives the claimed attempt back, and never fails the job for good. */
  refund: boolean
}

/** Runs the transaction of fail or requeue, and retries it after a `23505` on the pending-key index. */
async function returnClaim(
  database: Pick<Database, "execute" | "transaction">,
  table: JobTable,
  c: ClaimRef,
  o: ReturnOptions,
  stats: { uniqueViolations: number },
): Promise<FailOutcome> {
  for (let attempt = 0; ; attempt++) {
    try {
      return await database.transaction(
        async () => {
          const outcome = await returnOnce(database, table, c, o)
          await o.pause?.beforeCommit?.(database)
          return outcome
        },
        // A statement that waits for the lock of a row that a concurrent transaction changes then rechecks the new row, where a higher level fails with a serialization error.
        { isolation: "read committed" },
      )
    } catch (e) {
      // A producer inserted a pending job of the key after the competitor check: retry, now with a competitor.
      if (isUniqueViolation(e, "job_pending_key_idx") && attempt < (o.maxRetries ?? 5)) {
        stats.uniqueViolations++
        continue
      }
      throw e
    }
  }
}

/** Runs the statements of one transaction of fail or requeue. */
async function returnOnce(
  tx: StatementTarget,
  table: JobTable,
  c: ClaimRef,
  o: ReturnOptions,
): Promise<FailOutcome> {
  const now = o.now ?? null
  const own = await query(
    tx,
    `select id, scope, task, key, attempts, max_attempts from ${table.name}
     where id = $1::text::bigint and claim_id = $2::text::bigint and state = 'active' for update`,
    [c.id, c.claimId],
  )
  const a = own[0]
  if (!a) return "fenced"
  await o.pause?.afterOwnLock?.(tx)
  // A terminal outcome is always recorded as failed and never merged away; failed rows are outside key uniqueness.
  if (o.terminal || (!o.refund && Number(a.attempts) >= Number(a.max_attempts))) {
    await query(
      tx,
      `update ${table.name} set state = 'failed', claim_id = null, locked_by = null, lease_until = null, last_error = $3::text,
         failed_at = ${NOW}
       where id = $2::text::bigint`,
      [now, c.id, o.error],
    )
    return "failed"
  }
  // A null error keeps the error of the latest failure, since a requeue records none.
  const toPending = () =>
    query(
      tx,
      `update ${table.name} set state = 'pending', attempts = attempts - $5::text::int,
         run_at = n.t + make_interval(secs => $3::text::float8), not_before = n.t + make_interval(secs => $3::text::float8),
         window_started_at = n.t, claim_id = null, locked_by = null, lease_until = null, last_error = coalesce($4::text, last_error)
       from (select ${NOW} as t) n
       where id = $2::text::bigint`,
      [now, c.id, o.backoff, o.error, o.refund ? 1 : 0],
    )
  if (a.key == null) {
    await toPending()
    return "retry"
  }
  const competitor = await query(
    tx,
    `select id, key_mode from ${table.name} where state = 'pending' and scope = $1::text and task = $2::text and key = $3::text
     for update`,
    [a.scope, a.task, a.key],
  )
  await o.pause?.afterCompetitorCheck?.(tx)
  const b = competitor[0]
  if (!b) {
    await toPending()
    return "retry"
  }
  // The pending row carries the mode of the latest enqueue, which is the newest producer code during a rolling deploy.
  // Attempts stay with their rows: neither branch moves them.
  if (b.key_mode === "keep") {
    await query(tx, `delete from ${table.name} where id = $1::text::bigint`, [b.id])
    await toPending()
    return "replaced-pending"
  }
  await query(tx, `delete from ${table.name} where id = $1::text::bigint`, [c.id])
  return "superseded"
}
