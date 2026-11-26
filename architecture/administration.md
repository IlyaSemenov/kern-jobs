# Administration

An application retries failed jobs and deletes pending or failed ones by id through `retry` and `delete` of `jobs`, deletes the pending jobs of a scope, a task, or a key through `deletePending`, and deletes the failed jobs of a scope or a task, or by age, through `deleteFailed`, which run on its own client as the [enqueue](enqueue.md) does: each is one statement, since the client, such as a `pg.Pool`, gives kern-jobs no transaction of its own, and it joins the transaction of the application that the client holds.
None of them changes an active row, since the database cannot stop the handler of a running job.
A failure job is an ordinary job of its failure task, which they reach as any job of that task, the `failure.task` of the implementation, and none of them changes a failure job by the job whose failure inserted it ([Failure jobs](failure-jobs.md#consequences-of-ordinary-jobs)).

The application inspects failed jobs by reading the table ([Data model](data-model.md#columns)).
It deletes pending and failed jobs through `jobs` or by a statement of its own ([Delete from SQL](#delete-from-sql)), and must change the table otherwise only through `jobs` and the SQL functions [`enqueue` and `enqueue_items`](enqueue.md#sql-function) and [`retry`](#sql-function), since workers and enqueues rely on the rows that they write.

## Retry

Retry returns a `failed` job to pending in place, and returns `null` for an id of another state or of no job:

- The job keeps every column but those of a new cycle of attempts: its id, task, scope, key, key mode, queue, payload, `max_attempts`, `created_at`, `locked_at`, and `last_error`, which tells why it failed until it runs again.
- `attempts` becomes 0, a full budget, so that a retry after a fix of its cause survives transient errors again ([decision](../decisions/administration/retry-resets-attempts.md)).
- `run_at` and `window_started_at` become now, `not_before` and `failed_at` null, as a new enqueue sets them: a retry has no backoff.
- When a pending job of its `(scope, task, key)` that is not sealed exists, it changes neither job and returns the id of the pending one, in every key mode, so that the operator decides which of them runs ([decision](../decisions/administration/retry-keeps-both-jobs.md)).
- A batch job returns sealed whatever the jobs of its key, since its items are in no other job, with the items that it had when it failed, which [partial failures](settlement.md#partial-failure-of-a-batch) may have reduced.

The statement locks the failed row and the pending job of its key, and updates the failed row only without such a job ([decision](../decisions/administration/retry-updates-the-row.md)):

- A conflict that the statement sees is its result, so the transaction of the client stays usable.
- A pending job of the key that a producer inserts after the snapshot of the statement, or holds uncommitted until the update, makes the unique index of pending keys reject the update with `23505`, which reaches the application and aborts its transaction; nothing changes.
- A claim that holds the pending job of the key makes the retry wait, and after the claim commits, that job is active, and the failed job returns to pending.
- A second retry of the job waits for the first and finds the job no longer failed.

A retried job is a pending job of its key like one that a producer inserts, so a fail that returns a job of the key to pending, or the reaper, which conflict with it, retry after `23505` as with a producer ([Settlement](settlement.md#fail), [Leases](leases.md#reaper)).

### SQL function

The function `retry` of the schema retries a failed job from SQL, such as from psql, by a copy of the statement of retry, and returns `retried` and `pending_id` as `OUT` parameters.

- It takes the id of a job and no scope, and acts on a failed job of any scope, as `jobs` with `ALL_SCOPES` does.
- It returns a row of `retried` true, or false with the id of the pending job of the key as `pending_id`, and no row when no failed job has the id.
- Its time is `statement_timestamp()`, the start of the statement of the caller.
- Each call runs its statement in the transaction of the caller, so the calls of one statement for the failed jobs of a query retry one after another, in an order that the plan of the query decides: of the failed jobs of a key that are not batch jobs, at most one returns to pending, and a call that finds a pending job of the key that is not sealed returns its id as `pending_id`.
- Its body is a SQL-standard body, bound to the table at its creation, as that of [`enqueue`](enqueue.md#sql-function) is, so a call depends neither on the `search_path` of the caller nor on the name of the schema; it is `SECURITY INVOKER`, so it runs with the privileges of its caller.
- The statement of the core and the function are two copies, which a test compares on the same jobs ([decision](../decisions/enqueue/sql-function-copies-the-statement.md)): a change of retry must come with a migration that replaces the function.

## Delete

Delete deletes a `pending` or `failed` job, and returns whether it deleted it.

- A delete that meets a row that another transaction holds, such as a claim, a producer, or a fail transaction that locked it as the pending job of its key, waits for that transaction and deletes the row only if it is still pending or failed, at `READ COMMITTED` ([PostgreSQL](postgresql.md#isolation)).
- A delete of a pending job of a key deletes the events that producers merged into it, also one whose producer committed while the delete waited for it.

## Delete pending jobs

`deletePending()`, `deletePending({ task })`, and `deletePending({ task, key })` delete the pending jobs of a scope, of a task, or of a key, and return their count, so that the application cancels jobs waiting to run, including retries, such as a reminder of a key, the jobs of a retired task, or the jobs of a deleted tenant, without reading their ids from the table.

- With a scoped task, the scope is that of `jobs`, with the checks and errors of an [enqueue](producer.md#scope), or every scope with `ALL_SCOPES`, which an enqueue rejects, also with a key; with another task, it is the empty scope, also when `jobs` has a scope.
- Without a filter, the scope is that of [retry and delete](#scope).
- Only a call without an argument deletes the jobs of every task, and a filter must be an object of a task and, optionally, a key that is a string or `true`, the empty key of an [enqueue](producer.md#options), without other own properties, so that a mistaken argument, such as `undefined`, a promise of a filter, a task, a misspelled field, or a key that is `undefined`, is rejected rather than widen the delete.
- It deletes only pending rows, and with a key at most one in each scope, which the unique index of the open keys keeps single per scope, task, and key, besides the sealed batch jobs of the key; active and failed jobs stay.
- The statement has a condition only for each given column, and with a key the condition `not sealed or sealed`, so that a filter by key can use `job_open_key_idx` and `job_sealed_key_idx` also in a generic plan of a prepared statement.
- A pending job that a claim holds is deleted only if it is still pending after the claim ends, as with [delete](#delete), so a job that starts first is not deleted.
- A pending job that a producer adds after the snapshot of the statement stays, as an enqueue after the delete does, and so does the job that a fail transaction returns to pending in place of the pending job of mode `keep` that it deletes while the statement waits for it ([Settlement](settlement.md#fail)): the count tells only the rows that the statement deleted.
- It reads the task, its name, and the key once and checks the name and the key as an enqueue does, before the client is called, so the statement gets the checked name also from a task whose `name` is a getter.

## Delete failed jobs

`deleteFailed()`, `deleteFailed({ task })`, `deleteFailed({ olderThan })`, and `deleteFailed({ task, olderThan })` delete the failed jobs of a scope, filtered by task, by age, or both, and return their count, so that the application deletes old dead letters regularly, or those of a retired task or of a deleted tenant, without reading their ids from the table.
With `olderThan`, it deletes only the jobs that failed more than that many seconds ago.

- The scope is that of [`deletePending`](#delete-pending-jobs): with a task, that of the task, and without one, that of [retry and delete](#scope), also with `olderThan` alone.
- Only a call without an argument deletes the failed jobs of every task at any age, and a filter must name a task, `olderThan`, or both, without other own properties, and is read and checked as the filter of `deletePending` is, so that a mistaken argument, such as `undefined`, `{}`, or a task or `olderThan` that is `undefined`, is rejected rather than widen the delete.
- `olderThan` takes the range of a delay, seconds from 0 to 1e12, checked before the client is called; the statement compares `failed_at` plus `olderThan` with its own time, by the clock of the database, since its time less 1e12 seconds is before the earliest timestamp of PostgreSQL.
- It deletes only failed rows; pending and active jobs stay.
- A failed job that a retry holds is deleted only if it is still failed after the retry ends, at `READ COMMITTED` ([PostgreSQL](postgresql.md#isolation)), so a job that a retry returns to pending is not deleted.

## Delete from SQL

An application deletes pending and failed jobs also by a `DELETE` of its own on the table, where it has no `jobs`, such as in a migration of its own or from psql, and filters them by any condition, such as on the payload ([decision](../decisions/administration/delete-from-sql.md)).

- The statement must limit the delete to pending or failed rows by a condition on `state`, with which it deletes them as [delete](#delete), `deletePending`, and `deleteFailed` do: it rechecks a row that another transaction holds at `READ COMMITTED` ([PostgreSQL](postgresql.md#isolation)), and a pending job of a key takes with it the events that merged into it.
- A delete of an active row would break the guarantees of its claim: its handler can run on, the next job of its queue can start beside it, and its terminal failure inserts no failure job, since no fail records it.
- The application must not update the table, since a retry changes the columns of a new cycle of attempts together and handles the pending job of its key ([Retry](#retry)), and the check constraints and the unique indexes reject only some mistaken updates.
- The table `job` of the schema of the installation and the columns that the user documentation names for such a delete, `id`, `state`, `task`, `scope`, `key`, `payload`, and `failed_at`, with their types and values, are a contract that a migration keeps ([Migrations](migrations.md#history)).

## Scope

With a `scope` of `createJobs`, retry, delete, and, without a task, `deletePending` and `deleteFailed` act only on the jobs of that scope, and resolve it at each call with the checks of an enqueue, so a function that throws or returns an invalid scope rejects the call before the client is called.
Without one, they act only on unscoped jobs, whose stored scope is empty, and with `ALL_SCOPES`, an exported symbol, on the jobs of every scope, so that a jobs API that misses its scope never reaches the jobs of a tenant ([decision](../decisions/administration/scope-limits-administration.md)).
A job of another scope gives the result of no job.
Retry, delete, and, without a task, `deletePending` and `deleteFailed` match the stored scope of a job, not the definition of its task, so a job without a scope of a task that became scoped is reached through `jobs` without a scope, and a job of a scope of a task that is no longer scoped, through `jobs` of that scope or with `ALL_SCOPES`.
`ALL_SCOPES` is `Symbol.for("kern-jobs.ALL_SCOPES")`, so that two copies of the package in one process share it.
