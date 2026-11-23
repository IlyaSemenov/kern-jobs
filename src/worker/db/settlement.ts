// Settlement of a claimed job: complete, fail with retry, merge, or a final failure, and requeue of a job whose attempt is given back.
import type { JobTable } from "../../sql/job-table"
import { NOW } from "../../sql/postgres"
import { labelSql, toStatementLabel } from "../../sql/statement"
import type { FailureReason } from "../../task/failure"

import type { ClaimRef } from "./claim"
import type { WorkerDb } from "./db"
import { insertFailureJobsSql } from "./failure"
import { query, type StatementTarget } from "./query"
import { isUniqueViolation } from "./violation"

/** Completes a job by deleting its active row; `false` means the claim was lost. */
export async function complete(db: WorkerDb, c: ClaimRef): Promise<boolean> {
  const count = await db.job
    .where({ id: BigInt(c.id), claimId: BigInt(c.claimId), state: "active" })
    .comment(toStatementLabel("settle:complete"))
    .delete()
  return count === 1
}

/** Outcome of a fail. */
export type FailOutcome = "fenced" | "failed" | "retry" | "superseded" | "replaced-pending"

/** Reason of a fail, which the `JobFailure` of a failure job records if the fail is terminal: `terminal-error` fails the job for good, without retries; `handler-error` when omitted. */
export type FailReason = Extract<FailureReason, "handler-error" | "terminal-error" | "late-claim">

/** Options of a fail. */
export interface FailOptions {
  error: string
  reason?: FailReason | undefined
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
 * Settles an active job after an error: `failed` on a terminal error or the last attempt, with its failure job, if its claim stamped one, inserted by the same statement, otherwise back to pending with a backoff, or merged with the pending job of its key by the key mode of that job.
 * Runs one `READ COMMITTED` transaction of the database, which reserves a connection for it, so statements of other handlers never run inside it.
 * A lost connection throws `TransactionConnectionError` and is never retried on.
 */
export async function fail(
  db: WorkerDb,
  table: JobTable,
  c: ClaimRef,
  o: FailOptions,
  stats: { uniqueViolations: number } = { uniqueViolations: 0 },
): Promise<FailOutcome> {
  return await returnClaim(db, table, c, { ...o, refund: false }, stats)
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
  db: WorkerDb,
  table: JobTable,
  c: ClaimRef,
  o: RequeueOptions = {},
  stats: { uniqueViolations: number } = { uniqueViolations: 0 },
): Promise<RequeueOutcome> {
  const outcome = await returnClaim(
    db,
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
  db: WorkerDb,
  table: JobTable,
  c: ClaimRef,
  o: ReturnOptions,
  stats: { uniqueViolations: number },
): Promise<FailOutcome> {
  for (let attempt = 0; ; attempt++) {
    try {
      return await db.$transaction(
        async () => {
          const outcome = await returnOnce(db, table, c, o)
          await o.pause?.beforeCommit?.(db.$database)
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
  db: WorkerDb,
  table: JobTable,
  c: ClaimRef,
  o: ReturnOptions,
): Promise<FailOutcome> {
  // Raw statements on the database of `db` join the ambient transaction of `db.$transaction`, as its typed queries do.
  const tx = db.$database
  const now = o.now ?? null
  const a = await db.job
    .where({ id: BigInt(c.id), claimId: BigInt(c.claimId), state: "active" })
    .select("id", "scope", "task", "key", "attempts", "maxAttempts")
    .forUpdate()
    .take()
    .optional()
    .comment(toStatementLabel("settle:lock-claim"))
  if (!a) return "fenced"
  await o.pause?.afterOwnLock?.(tx)
  // A terminal outcome is always recorded as failed and never merged away; failed rows are outside key uniqueness.
  // The failure job is inserted by the statement that fails the job, so both commit or roll back together, and only a claim that commits its own transition from active inserts one.
  if (o.reason === "terminal-error" || (!o.refund && a.attempts >= a.maxAttempts)) {
    await query(
      tx,
      labelSql(
        "settle:fail",
        `with s as (
         update ${table.name} set state = 'failed', claim_id = null, locked_by = null, lease_until = null, last_error = $3::text,
           failed_at = ${NOW}
         where id = $2::text::bigint
         returning *, $4::text::bigint as failed_claim_id
       ), i as (
         ${insertFailureJobsSql(table, "s", "$5::text")}
       )
       select from s`,
      ),
      [now, c.id, o.error, c.claimId, o.reason ?? "handler-error"],
    )
    return "failed"
  }
  // A null error keeps the error of the latest failure, since a requeue records none.
  const toPending = () =>
    query(
      tx,
      labelSql(
        "settle:to-pending",
        `update ${table.name} set state = 'pending', attempts = attempts - $5::text::int,
         run_at = n.t + make_interval(secs => $3::text::float8), not_before = n.t + make_interval(secs => $3::text::float8),
         window_started_at = n.t, claim_id = null, locked_by = null, lease_until = null, last_error = coalesce($4::text, last_error)
       from (select ${NOW} as t) n
       where id = $2::text::bigint`,
      ),
      [now, c.id, o.backoff, o.error, o.refund ? 1 : 0],
    )
  if (a.key === null) {
    await toPending()
    return "retry"
  }
  // The pending-key index allows at most one competitor.
  const b = await db.job
    .where({ state: "pending", scope: a.scope, task: a.task, key: a.key })
    .select("id", "keyMode")
    .forUpdate()
    .take()
    .optional()
    .comment(toStatementLabel("settle:lock-competitor"))
  await o.pause?.afterCompetitorCheck?.(tx)
  if (!b) {
    await toPending()
    return "retry"
  }
  // The pending row carries the mode of the latest enqueue, which is the newest producer code during a rolling deploy.
  // Attempts stay with their rows: neither branch moves them.
  if (b.keyMode === "keep") {
    await db.job.find(b.id).comment(toStatementLabel("settle:delete-competitor")).delete()
    await toPending()
    return "replaced-pending"
  }
  await db.job.find(a.id).comment(toStatementLabel("settle:delete-claim")).delete()
  return "superseded"
}
