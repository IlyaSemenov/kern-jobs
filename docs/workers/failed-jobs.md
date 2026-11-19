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

To give up a failed job, or to keep a pending one from running, delete it:

```ts
await jobs.delete(id)
```

A pending job of a key carries the events that the enqueues of its key merged into it, so they are deleted with it; an enqueue after the delete adds a new job.

`delete` resolves to `false` when no pending or failed job has the id, such as one that has completed or that a worker runs.
A worker cannot stop a running handler at the request of the database, so wait until the job ends: if it fails, or returns to pending for another attempt, delete it then.

## Delete pending jobs

To keep the pending jobs of a task from running, such as of a task that no worker implements any more, delete them without looking up their ids:

```ts
await jobs.deletePending({ task: syncLegacyCrmTask })
```

For a scoped task, this deletes its jobs in the scope of `jobs`; to delete them in every scope, call it through `jobs` with [`ALL_SCOPES`](#jobs-of-a-scope).

To delete the pending jobs of every task of a [scope](../tasks/scopes), such as when you delete the data of a tenant, call it without a filter through `jobs` with that scope, in the [transaction](../database-clients#transactions-without-an-orm) that deletes the data:

```ts
await sql.begin(async tx => {
  await tx`delete from product where tenant_id = ${tenant.id}`
  await createJobs(tx, { scope: tenant.id }).deletePending()
})
```

Through `jobs` with [`ALL_SCOPES`](#jobs-of-a-scope), it deletes the pending jobs of every scope, and through `jobs` without a scope, only unscoped jobs.

To delete the pending job of a key, pass the task and the key, as in [Cancel a pending job](../tasks/keys#cancel-a-pending-job).

`deletePending` resolves to the number of the jobs it deleted, and leaves the jobs that run or failed.
Each job takes with it the events that the enqueues of its key merged into it.

## Delete old failed jobs

Failed jobs stay until you delete them, so delete the old ones regularly, such as every night, with a statement on the table:

```sql
delete from kern_jobs.job where state = 'failed' and failed_at < now() - interval '30 days'
```

It is the only statement on the table that may change it besides kern-jobs: keep `state = 'failed'` in it, since workers and enqueues rely on the rows of the other jobs.

## Jobs of a scope

Through `jobs` with a [scope](../tasks/scopes), `retry`, `delete`, and `deletePending` without a filter act only on the jobs of that scope and call its function each time, so a function that reads the scope of the current request needs a request.
Through `jobs` without a scope, they act only on [unscoped](../tasks/scopes) jobs.

To manage the jobs of every scope, such as in a script of an operator, pass `ALL_SCOPES` as the scope:

```ts
import { ALL_SCOPES, createJobs } from "kern-jobs"

const allJobs = createJobs(sql, { scope: ALL_SCOPES })

await allJobs.retry(id)
```

`deletePending` of a scoped task deletes its jobs of the scope of `jobs`, or of every scope with `ALL_SCOPES`, and `deletePending` of another task deletes its unscoped jobs through any `jobs`.
`jobs` with `ALL_SCOPES` deletes the jobs of a scoped task, but cannot enqueue them.
