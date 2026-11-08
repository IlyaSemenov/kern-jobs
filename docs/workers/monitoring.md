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
The other sources are failures of the worker itself, such as a lost database connection, after which the worker keeps trying.

Without `onError`, the worker writes the errors with `console.error`.

## Check the health of a worker

To tell your process manager or orchestrator that the worker has started, await `ready`:

```ts
await worker.ready
```

It resolves once the worker has checked its schema and taken jobs for the first time, or found none, so the database, the schema, and the connections of your client work.
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
- `ready`: the worker takes jobs when its slots are free.
- `unhealthy`: the worker cannot reach the database in time, such as after losing its connection: it takes no new jobs and keeps trying, and it becomes `ready` again by itself once the database answers.
- `stopping` and `stopped`: `stop` has begun or ended.

`state` does not tell whether jobs progress: a worker whose handlers keep every connection of its client busy stays `ready` while it waits for one.
Watch the age of the oldest due job, in [Watch the backlog](#watch-the-backlog), to catch that.

## Watch the backlog

To watch the backlog and the failures, such as in your metrics, query the job table:

```sql
select
  task,
  count(*) filter (where state = 'pending' and run_at <= now()) as due,
  count(*) filter (where state = 'pending' and run_at > now()) as scheduled,
  count(*) filter (where state = 'pending' and attempts > 0) as retrying,
  count(*) filter (where state = 'active') as running,
  count(*) filter (where state = 'failed') as failed,
  coalesce(extract(epoch from now() - min(run_at) filter (where state = 'pending' and run_at <= now())), 0) as oldest_due_seconds
from kern_jobs.job
group by task
```

- `due`: jobs that wait for a worker.
- `scheduled`: jobs that wait for their time, such as a delay or the backoff of a retry.
- `retrying`: pending jobs after a failed attempt.
- `running`: jobs that workers run.
- `failed`: jobs that spent their attempts or failed for good, which stay until you [retry or delete them](./failed-jobs).
- `oldest_due_seconds`: how long the oldest due job waits.

A growing `oldest_due_seconds` means that the workers do not keep up, or that no worker takes the jobs of the task, such as when none runs or implements it.
A job of a [queue](../tasks/queues) also waits for the jobs before it in its queue.

A worker deletes a job when it completes, so the table does not count completed jobs.
To count the errors of handlers as they happen, count the `handler` reports of [`onError`](#report-errors).

Read the table by your queries, retry and delete jobs through [`jobs`](./failed-jobs), and delete [old failed jobs](./failed-jobs#delete-old-failed-jobs) only by the statement that it shows: the worker relies on the rows that it and the enqueue write.
