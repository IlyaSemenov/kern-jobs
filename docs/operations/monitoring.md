# Monitoring

## Report errors

To send the errors of handlers and of the worker to your error reporting, pass `onError`:

```ts
const worker = startWorker(driver, {
  manifest,
  onError(error, info) {
    reportError(error, { extra: info })
  },
})
```

`info.source` tells what failed.
For `handler`, the handler of a job threw, and `info` has the task and the job.
For `schedule`, the worker could not check the schedule identified by `info.id` and tries again in its next round.
The other sources are failures of the worker itself, such as a lost database connection, after which the worker keeps trying.

Without `onError`, the worker writes the errors with `console.error`.

### Report the first and terminal attempts

A job that keeps failing reports the error of each of its attempts.
To report only its first attempt and the one that fails it for good, check `info.decision`:

```ts
const worker = startWorker(driver, {
  manifest,
  onError(error, info) {
    if (info.source === "handler" && info.decision === "retry" && info.job.attempt > 1) return
    reportError(error, { extra: info })
  },
})
```

`info.decision` is what the worker decided when the handler threw: `retry` to run the job again, or `terminal` to fail it for good, by a [`TerminalError`](../workers/retries#decide-in-the-handler) or on its last attempt.

It is a decision, not a record of what became of the job: after `retry`, a newer job of its [key](../tasks/keys#while-a-job-of-the-key-runs) can run in its place, and a job can fail for good without an error of its handler, such as when its worker dies on its last attempt.
So use `decision` to choose what you report, and [`onFailure`](../workers/failure-handlers) to act on a failure for good.

## Check the health of a worker

To tell your process manager or orchestrator that the worker has started, await `ready`:

```ts
await worker.ready
```

It resolves once the worker has checked its schema and reached the database on the connections of your client, and no other running worker holds its `id`.
Until then, the worker [reports](#report-errors) what keeps it from starting, such as a schema that `migrateJobs` has not created.
If `stop` begins first, `ready` rejects.

For a health check that runs repeatedly, read `state`:

```ts
import { createServer } from "node:http"

createServer((request, response) => {
  response.writeHead(worker.state === "ready" ? 200 : 503).end(worker.state)
}).listen(8080)
```

- `starting`: the worker is not ready yet.
- `ready`: the worker has started, and its heartbeat is healthy.
- `unhealthy`: the latest heartbeat of the worker failed or is late, such as after losing its connection or while another running worker holds its `id`: it takes no new jobs and keeps trying, and it becomes `ready` again by itself once a heartbeat succeeds.
- `stopping` and `stopped`: `stop` has begun or ended.

`state` does not tell whether jobs progress: a worker whose handlers keep every connection of its client busy stays `ready` while it waits for one.
Watch the age of the oldest due job, in [Watch the backlog](#watch-the-backlog), to catch that.

Neither `ready` nor `state` tells whether the worker checks its [schedules](../tasks/schedules): a worker stays `ready` while every check of a schedule fails.
Alert on `schedule` errors in `onError` to catch that.

## Watch the backlog

To watch the backlog and the failures, such as in your metrics, query the view `job_summary` in the schema of kern-jobs:

```sql
select
  task,
  sum(due) as due,
  sum(scheduled) as scheduled,
  sum(retrying) as retrying,
  sum(running) as running,
  sum(failed) as failed,
  max(oldest_due_seconds) as oldest_due_seconds
from kern_jobs.job_summary
group by task
```

The view has a row for each task in each scope that has jobs:

- `scope`: the [scope](../tasks/scopes) of the jobs, or an empty string for jobs without one.
- `task`: the name of the task.
- `due`: jobs that wait for a worker.
- `scheduled`: jobs that wait for their time, such as a delay or a debounce.
- `retrying`: jobs that wait for the backoff of a [retry](../workers/retries).
- `running`: jobs that workers run.
- `failed`: jobs that spent their attempts or failed for good, which stay until you [retry or delete them](./managing-jobs).
- `oldest_due_seconds`: how long the oldest due job waits.
- `longest_running_seconds`: how long ago a worker took the longest running job.
- `oldest_failed_seconds`: how long ago the oldest failed job failed.
- `most_attempts`: the most attempts that a job that waits or runs has spent, counting the attempt that runs.

Each age is null when the row has no job that it measures, and `most_attempts` is null when every job of the row has failed.
A task without jobs in a scope has no row, so take a missing value as zero where your metrics need one.

A growing `oldest_due_seconds` means that the workers do not keep up, or that no worker takes the jobs of the task, such as when none runs or implements it.
A job of a [queue](../tasks/queues) also waits for the jobs before it in its queue.
Compare `longest_running_seconds` with the usual run time of the task to find a handler that hangs, or a job of a worker that ended, which stays running until a worker returns it, as [Leases](../workers/leases) describes.

A summary of every task reads every job, so query it at the interval of your metrics rather than often.
Later versions of kern-jobs only add columns to the view, so select the columns by name.

A worker deletes a job when it completes, so the view does not count completed jobs.
To count the errors of handlers as they happen, count the `handler` reports of [`onError`](#report-errors).

Retry jobs through [`jobs.retry`](./managing-jobs#retry-a-failed-job) or [from SQL](./managing-jobs#retry-from-sql).
Delete jobs, including [old failed jobs](./managing-jobs#delete-failed-jobs), through `jobs` or [from SQL](./managing-jobs#delete-jobs-from-sql).

### Grant access to a monitoring role

To let the role of your metrics read the view, grant it the view alone:

```sql
grant usage on schema kern_jobs to monitoring;
grant select on kern_jobs.job_summary to monitoring;
```

The role reads the view without access to the job table, so it sees neither payloads nor errors of jobs.

## Find the statements of kern-jobs

To tell the statements of kern-jobs from those of your application, such as in `pg_stat_activity` or in the log of slow statements, filter by the comment that starts each of them:

```sql
select pid, wait_event_type, now() - query_start as running, query
from pg_stat_activity
where state = 'active' and query like '/* kern-jobs:%'
```

Run it as a role that sees the statements of the roles of your application, such as one granted `pg_read_all_stats`: PostgreSQL shows another role `<insufficient privilege>` in place of their text.

The comment names the statement, such as `/* kern-jobs:claim:plain */` of a worker that takes jobs, or `/* kern-jobs:enqueue:one */` of an enqueue on a connection of your client.
To find the statements of one area, filter by its name, such as `'/* kern-jobs:settle:%'` for the statements that record the results of jobs; the areas are `enqueue`, `admin`, `claim`, `heartbeat`, `worker`, `reap`, `settle`, and `schedule`.

Transaction control, such as `BEGIN` and `COMMIT`, the settings of a transaction or a connection, the lock of the worker `id`, migrations, and the check of the schema when a worker starts carry no comment.
