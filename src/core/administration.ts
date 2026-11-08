// Administration of jobs by the application: retry of a failed job and delete of a pending or failed one, each in one statement on the client of the application.
import { hook, NOW } from "./db"
import type { JobTable } from "./schema"
import { queryValues, type ValuesTarget } from "./statement"

/** Result of a retry of a failed job: it returned to pending, or a pending job of its key kept it from returning. */
export type RetryResult = { retried: true } | { retried: false; pendingId: string }

/** Parameters of a retry. */
export interface RetryParams {
  id: string
  /** Scope that the job must have; any scope when omitted. */
  scope?: string | null
  /** Time of the statement; `statement_timestamp()` when omitted. */
  now?: Date | null
}

/**
 * Returns a failed job to pending in place, with a full budget of attempts, to run at once.
 * When a pending job of its key exists, it changes neither job and returns the id of the pending one.
 * Returns `null` when no failed job of the scope has the id.
 * A pending job of the key that a producer inserts after the snapshot of the statement makes it fail with the unique violation of `job_pending_key_idx`.
 */
export async function retryFailedJob(
  db: ValuesTarget,
  table: JobTable,
  o: RetryParams,
): Promise<RetryResult | null> {
  const rows = await queryValues(
    db,
    // One statement, since the client of the application gives no transaction of its own.
    // The pending job of the key is locked, so that a claim that holds it is waited for and, once it commits, no longer conflicts; the unique index of pending keys still rejects one that the snapshot misses.
    // The run time, the window, and the absent backoff are those of a new enqueue; the other columns stay.
    `with f as (
       select id, scope, task, key from ${table.name}
       where id = $2::text::bigint and state = 'failed' and ($3::text is null or scope = $3::text) and ${hook("retry")}
       for update
     ), c as (
       select p.id from ${table.name} p, f
       where p.state = 'pending' and p.scope = f.scope and p.task = f.task and p.key = f.key
       for update of p
     ), u as (
       update ${table.name} j set state = 'pending', attempts = 0, run_at = n.t, not_before = null, window_started_at = n.t,
         failed_at = null
       from f, (select ${NOW} as t) n
       where j.id = f.id and not exists (select from c)
       returning j.id
     )
     select json_build_array((select id::text from c), exists (select from u))::text from f`,
    [o.now ?? null, o.id, o.scope ?? null],
  )
  if (rows.length === 0) return null
  const [pendingId, retried] = JSON.parse(rows[0]![0]!) as [string | null, boolean]
  return retried ? { retried: true } : { retried: false, pendingId: pendingId! }
}

/**
 * Deletes a pending or failed job, and returns `false` when no such job of the scope, or of any scope when it is omitted, has the id.
 * A row that another transaction holds, such as a claim, is deleted only if it is still pending or failed after that transaction ends.
 * An active row is never deleted: the database cannot stop the handler of a running job.
 */
export async function deleteJob(
  db: ValuesTarget,
  table: JobTable,
  o: { id: string; scope?: string | null },
): Promise<boolean> {
  const rows = await queryValues(
    db,
    `delete from ${table.name}
     where id = $1::text::bigint and state in ('pending', 'failed') and ($2::text is null or scope = $2::text)
     returning id::text`,
    [o.id, o.scope ?? null],
  )
  return rows.length === 1
}
