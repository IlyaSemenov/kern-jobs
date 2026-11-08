# Failed Jobs

A job that spent its attempts or failed for good, such as by a `TerminalError`, stays in the database as failed until you retry or delete it.

## Find out why a job failed

Query the failed jobs with their errors:

```sql
select id, task, scope, key, attempts, last_error, failed_at, payload
from kern_jobs.job
where state = 'failed'
order by failed_at desc
limit 100
```

- `last_error`: the message of the error of the last attempt, or of the worker, such as `lease expired` when its worker died or lost the job.
- `failed_at`: when the job failed.
- `attempts`: the attempts that the job ran.

The table keeps only the message: for an error of the handler, [`onError`](./monitoring#report-errors) gets the error itself, such as with its stack trace, and `info.job.id` is the id of the job.

Jobs that keep failing but still have attempts left wait for their next attempt as pending: find them by `state = 'pending' and attempts > 0`.

## Retry a failed job

Once you have fixed the cause, such as a defect of the handler, retry the job by its id:

```ts
await jobs.retry(id)
```

The job returns to pending with its payload and options and the full budget of its `maxAttempts`, and runs as soon as a worker is free, from its first attempt; `retry` resolves to `{ retried: true }`.
A job of a [queue](../tasks/queues) waits behind the jobs of its queue that are due.

When a pending job of its [key](../tasks/keys) exists, such as one that an enqueue added after the failure, `retry` changes neither job and resolves to `{ retried: false, pendingId }`.
Then delete the failed job if the pending job does its work, or delete the pending job and retry the failed one again.

`retry` resolves to `null` when no failed job has the id, such as one that a retry has already returned to pending.

An enqueue that adds a new pending job of its key and commits while `retry` runs can make it reject with the unique violation of your database client and change nothing.
In a transaction, the error aborts the transaction: roll it back, and retry the job again.

## Delete a job

To give up a failed job, or to keep a pending one from running, such as a job of a task that no worker implements any more, delete it:

```ts
await jobs.delete(id)
```

A pending job of a key carries the events that the enqueues of its key merged into it, so they are deleted with it; an enqueue after the delete adds a new job.

`delete` resolves to `false` when no pending or failed job has the id, such as one that has completed or that a worker runs.
A worker cannot stop a running handler at the request of the database, so wait until the job ends: if it fails, or returns to pending for another attempt, delete it then.

## Delete old failed jobs

Failed jobs stay until you delete them, so delete the old ones regularly, such as every night, with a statement on the table:

```sql
delete from kern_jobs.job where state = 'failed' and failed_at < now() - interval '30 days'
```

It is the only statement on the table that may change it besides kern-jobs: keep `state = 'failed'` in it, since workers and enqueues rely on the rows of the other jobs.

## Jobs of a scope

Through `jobs` with a [scope](../tasks/scopes), `retry` and `delete` act only on the jobs of that scope and call its function each time, so a function that reads the scope of the current request needs a request.
Through `jobs` without a scope, they act on the jobs of every scope: use such `jobs` for the jobs of the tasks that are not scoped, and in scripts that manage the jobs of every scope.
