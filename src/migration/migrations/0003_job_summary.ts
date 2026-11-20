// Third migration of kern-jobs: the view `job_summary`, a public summary of the jobs of each task in each scope for metrics.
// Its columns are a public contract: a later migration changes the view by `replaceView`, which keeps its grants, and only appends columns.
import { createView, defineMigration } from "kern-migrations"

export default defineMigration(
  // Every condition takes the time of the statement, as the statements of the core do.
  // A job in the backoff of a retry has `not_before` ahead, and `run_at` is never below `not_before`, so `due`, `scheduled`, and `retrying` split the pending jobs.
  createView(
    "job_summary",
    `
select
  scope,
  task,
  count(*) filter (where state = 'pending' and run_at <= statement_timestamp()) as due,
  count(*) filter (
    where state = 'pending' and run_at > statement_timestamp()
      and (not_before is null or not_before <= statement_timestamp())
  ) as scheduled,
  count(*) filter (where state = 'pending' and not_before > statement_timestamp()) as retrying,
  count(*) filter (where state = 'active') as running,
  count(*) filter (where state = 'failed') as failed,
  extract(epoch from statement_timestamp() - min(run_at) filter (
    where state = 'pending' and run_at <= statement_timestamp()
  ))::float8 as oldest_due_seconds,
  extract(epoch from statement_timestamp() - min(locked_at) filter (where state = 'active'))::float8
    as longest_running_seconds,
  extract(epoch from statement_timestamp() - min(failed_at) filter (where state = 'failed'))::float8
    as oldest_failed_seconds,
  max(attempts) filter (where state in ('pending', 'active')) as most_attempts
from job
group by scope, task
`,
  ),
)
