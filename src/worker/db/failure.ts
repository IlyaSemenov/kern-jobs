// SQL for inserting a failure job, with the `JobFailure` of a terminal failure as its payload and the attempt budget of the failure job that the claim of the source job recorded.
import type { JobTable } from "../../sql/job-table"
import { FAILURE_TASK_SUFFIX } from "../../task/failure"

/**
 * SQL of the `JobFailure` of the failed row `s`, built from the stored row so that it records what the table committed: the payload as stored rather than as a handler may have changed it, ids as text, and the time in UTC whatever `TimeZone` and `DateStyle`.
 * The top-level `payload` is omitted for a row whose payload is SQL NULL, while a JSON `null` stays; stripping nulls in general would also drop the nulls inside the payload.
 */
function jobFailureSql(reason: string): string {
  return `jsonb_build_object(
      'id', s.failed_claim_id::text,
      'reason', ${reason},
      'error', jsonb_build_object('message', s.last_error),
      'failedAt', to_char(s.failed_at at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
      'job', jsonb_build_object(
        'id', s.id::text, 'task', s.task, 'scope', nullif(s.scope, ''), 'attempt', s.attempts,
        'maxAttempts', s.max_attempts, 'key', s.key, 'queue', s.queue))
    || case when s.payload is null then '{}'::jsonb else jsonb_build_object('payload', s.payload) end`
}

/**
 * SQL of the statement, for a data-modifying CTE, that inserts a failure job for each row of `source` that became failed with the stamp of its claim.
 * `source` must give the columns of the job table after the fail, and `failed_claim_id`, the claim id that the fail cleared; `reason` is the SQL of the reason.
 * The failure job is a fresh pending job of the failure task of the task of the failed job, with the stamped attempt budget and the scope of the failed job, due at its failure, without a key or a queue: a null key and queue cannot meet the unique indexes of pending keys and active queues, so the insert never conflicts.
 * The stamp of an inactive row is ignored, so only the stamp that the failed claim wrote counts: the fail reads it from the row that it changes from active.
 */
export function insertFailureJobsSql(table: JobTable, source: string, reason: string): string {
  return `insert into ${table.name} (task, scope, payload, run_at, window_started_at, created_at, max_attempts)
     select s.task || '${FAILURE_TASK_SUFFIX}', s.scope, ${jobFailureSql(reason)}, s.failed_at, s.failed_at, s.failed_at, s.failure_max_attempts
     from ${source} s
     where s.state = 'failed' and s.failure_max_attempts is not null`
}
