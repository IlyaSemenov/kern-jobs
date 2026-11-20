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

Neither `ready` nor `state` tells whether the worker checks its [schedules](./schedules): a worker stays `ready` while every check of a schedule fails.
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
- `retrying`: jobs that wait for the backoff of a [retry](./retries).
- `running`: jobs that workers run.
- `failed`: jobs that spent their attempts or failed for good, which stay until you [retry or delete them](./failed-jobs).
- `oldest_due_seconds`: how long the oldest due job waits.
- `longest_running_seconds`: how long ago a worker took the longest running job.
- `oldest_failed_seconds`: how long ago the oldest failed job failed.
- `most_attempts`: the most attempts that a job that waits or runs has spent, counting the attempt that runs.

Each age is null when the row has no job that it measures, and `most_attempts` is null when every job of the row has failed.
A task without jobs in a scope has no row, so take a missing value as zero where your metrics need one.

A growing `oldest_due_seconds` means that the workers do not keep up, or that no worker takes the jobs of the task, such as when none runs or implements it.
A job of a [queue](../tasks/queues) also waits for the jobs before it in its queue.
`longest_running_seconds` counts from the moment a worker took the job, also while the worker records its result.
Compare it with the usual run time of the task to find a handler that hangs, or a job of a worker that ended, which stays running until a worker returns it, as [Leases](./leases) describes.

A summary of every task reads every job, so query it at the interval of your metrics rather than often.
Later versions of kern-jobs only add columns to the view, so select the columns by name.

A worker deletes a job when it completes, so the view does not count completed jobs.
To count the errors of handlers as they happen, count the `handler` reports of [`onError`](#report-errors).

Retry and delete jobs, also [old failed jobs](./failed-jobs#delete-failed-jobs), only through [`jobs`](./failed-jobs): the worker relies on the rows that it and the enqueue write.

### Grant access to a monitoring role

To let the role of your metrics read the view, grant it the view alone:

```sql
grant usage on schema kern_jobs to monitoring;
grant select on kern_jobs.job_summary to monitoring;
```

The role reads the view without access to the job table, so it sees neither payloads nor errors of jobs.
