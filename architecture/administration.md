# Administration

An application retries failed jobs and deletes pending or failed ones by id through `retry` and `delete` of `jobs`, and deletes the pending jobs of a scope, a task, or a key through `deletePending`, which run on its own client as the [enqueue](enqueue.md) does: each is one statement, since the client, such as a `pg.Pool`, gives kern-jobs no transaction of its own, and it joins the transaction of the application that the client holds.
None of them changes an active row, since the database cannot stop the handler of a running job.

The application inspects failed jobs by reading the table ([Data model](data-model.md#columns)) and deletes the old ones by a statement of its own, `delete … where state = 'failed' and failed_at < …`, which the user documentation gives as the only write of the application to the table.

## Retry

Retry returns a `failed` job to pending in place, and returns `null` for an id of another state or of no job:

- The job keeps every column but those of a new cycle of attempts: its id, task, scope, key, key mode, queue, payload, `max_attempts`, `created_at`, `locked_at`, and `last_error`, which tells why it failed until it runs again.
- `attempts` becomes 0, a full budget, so that a retry after a fix of its cause survives transient errors again ([decision](../decisions/administration/retry-resets-attempts.md)).
- `run_at` and `window_started_at` become now, `not_before` and `failed_at` null, as a new enqueue sets them: a retry has no backoff.
- When a pending job of its `(scope, task, key)` exists, it changes neither job and returns the id of the pending one, in every key mode, so that the operator decides which of them runs ([decision](../decisions/administration/retry-keeps-both-jobs.md)).

The statement locks the failed row and the pending job of its key, and updates the failed row only without such a job ([decision](../decisions/administration/retry-updates-the-row.md)):

- A conflict that the statement sees is its result, so the transaction of the client stays usable.
- A pending job of the key that a producer inserts after the snapshot of the statement, or holds uncommitted until the update, makes the unique index of pending keys reject the update with `23505`, which reaches the application and aborts its transaction; nothing changes.
- A claim that holds the pending job of the key makes the retry wait, and after the claim commits, that job is active, and the failed job returns to pending.
- A second retry of the job waits for the first and finds the job no longer failed.

A retried job is a pending job of its key like one that a producer inserts, so a fail that returns a job of the key to pending, or the reaper, which conflict with it, retry after `23505` as with a producer ([Settlement](settlement.md#fail), [Leases](leases.md#reaper)).

## Delete

Delete deletes a `pending` or `failed` job, and returns whether it deleted it.

- A delete that meets a row that another transaction holds, such as a claim, a producer, or a fail transaction that locked it as the pending job of its key, waits for that transaction and deletes the row only if it is still pending or failed, at `READ COMMITTED` ([PostgreSQL](postgresql.md#isolation)).
- A delete of a pending job of a key deletes the events that producers merged into it, also one whose producer committed while the delete waited for it.

## Delete pending jobs

`deletePending()`, `deletePending({ task })`, and `deletePending({ task, key })` delete the pending jobs of a scope, of a task, or of a key, and return their count, so that the application cancels jobs waiting to run, including retries, such as a reminder of a key, the jobs of a retired task, or the jobs of a deleted tenant, without reading their ids from the table.

- With a scoped task, the scope is that of `jobs`, with the checks and errors of an [enqueue](producer.md#scope), or every scope with `ALL_SCOPES`, which an enqueue rejects, also with a key; with another task, it is the empty scope, also when `jobs` has a scope.
- Without a filter, the scope is that of [retry and delete](#scope).
- Only a call without an argument deletes the jobs of every task, and a filter must be an object of a task and, optionally, a key that is a string, without other own properties, so that a mistaken argument, such as `undefined`, a promise of a filter, a task, a misspelled field, or a key that is `undefined`, is rejected rather than widen the delete.
- It deletes only pending rows, and with a key at most one in each scope, which the unique index of pending keys keeps single per scope, task, and key; active and failed jobs stay.
- The statement has a condition only for each given column, so that a filter by key can use the unique index of pending keys also in a generic plan of a prepared statement.
- A pending job that a claim holds is deleted only if it is still pending after the claim ends, as with [delete](#delete), so a job that starts first is not deleted.
- A pending job that a producer adds after the snapshot of the statement stays, as an enqueue after the delete does, and so does the job that a fail transaction returns to pending in place of the pending job of mode `keep` that it deletes while the statement waits for it ([Settlement](settlement.md#fail)): the count tells only the rows that the statement deleted.
- It reads the task, its name, and the key once and checks the name and the key as an enqueue does, before the client is called, so the statement gets the checked name also from a task whose `name` is a getter.

## Scope

With a `scope` of `createJobs`, retry, delete, and `deletePending` without a filter act only on the jobs of that scope, and resolve it at each call with the checks of an enqueue, so a function that throws or returns an invalid scope rejects the call before the client is called.
Without one, they act only on unscoped jobs, whose stored scope is empty, and with `ALL_SCOPES`, an exported symbol, on the jobs of every scope, so that a jobs API that misses its scope never reaches the jobs of a tenant ([decision](../decisions/administration/scope-limits-administration.md)).
A job of another scope gives the result of no job.
Retry, delete, and `deletePending` without a filter match the stored scope of a job, not the definition of its task, so a job without a scope of a task that became scoped is reached through `jobs` without a scope, and a job of a scope of a task that is no longer scoped, through `jobs` of that scope or with `ALL_SCOPES`.
`ALL_SCOPES` is `Symbol.for("kern-jobs.ALL_SCOPES")`, so that two copies of the package in one process share it.
