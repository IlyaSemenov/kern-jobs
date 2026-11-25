# A retry updates the row in one statement on the client of the application

Retry and delete run on the client of the application, as the enqueue does, each in one statement, since a client such as a `pg.Pool` gives kern-jobs no transaction of its own.
The retry locks the failed row and the pending job of its key, and updates the failed row in place only without such a job, so the row keeps every column that a new cycle of attempts does not reset:

```sql
with f as (select id, scope, task, key from job where id = $2 and state = 'failed' for update),
c as (select p.id from job p, f where p.state = 'pending' and (p.scope, p.task, p.key) = (f.scope, f.task, f.key) for update of p),
u as (
  update job j set state = 'pending', attempts = 0, run_at = now, not_before = null, window_started_at = now, failed_at = null
  from f where j.id = f.id and not exists (select from c) returning j.id
)
select (select id from c), exists (select from u) from f
```

A pending job of the key that a producer inserts after the snapshot of the statement, or holds uncommitted until the update, makes the unique index of pending keys reject the update with `23505`, which reaches the application and aborts its transaction.

Deleting the failed row and inserting it again with its id, with `ON CONFLICT` on the unique index of pending keys, never fails with `23505`, since `ON CONFLICT` waits for such a job and takes it as the conflict.
But `ON CONFLICT DO NOTHING` would still have deleted the failed row, so it fits only a retry that deletes the failed job when a pending job of its key exists, which [the retry does not do](retry-keeps-both-jobs.md); and the insert lists every column that it keeps, so a retry of the previous version of kern-jobs during a rolling deploy resets a column that a migration of the new version added.

The SQL function `retry` of the schema could update the row in a block of PL/pgSQL that catches the unique violation, and report the race as the conflict that it is, without aborting the transaction of the caller.
A PL/pgSQL body finds the table by the search path of its call rather than binding it at its creation ([decision](../enqueue/sql-function-binds-its-table.md)).
For `jobs.retry` to report the race the same way, the core would call the function, which would take the time and the scope too.
Arguments with defaults would add both to the signature that every caller of SQL sees, and keeping `retry(id)` as it is would take a second function of the schema with its own history in the migrations, for a race of a manual operation.

A function over a driver of kern-db, as `migrateJobs` takes it, could run the retry in a transaction of its own and retry it after `23505`.
It needs a new entry of the package, or the worker entry, and it cannot join the transaction of the application.
