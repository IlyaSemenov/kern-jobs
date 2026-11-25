# The SQL function copies the statement of the core

The SQL function `enqueue` holds a copy of the statement with which the core enqueues one job, and the jobs API keeps running its own statement.
A test runs the same sequences of enqueues, retries, and claims through both in two scopes of one table and compares their rows and results after each step, at the time of the statement of the function to the microsecond.
A change of the run time or the merge of a job changes both: the statement of the core, and the function by a new migration.
The SQL function `retry` copies the statement of retry in the same way, and its test compares the two on the same jobs.

## The core calls the function

The jobs API could enqueue one job by `select * from enqueue(…)`, so that one implementation serves both.
Graphile Worker does so: its `addJob` calls the SQL function `add_job` ([docs](https://github.com/graphile/worker/blob/v0.18.0/website/docs/sql-add-job.md)).

- `enqueueMany` would keep a statement of its own, one insert of the whole batch, so the merge of a job would still have two copies; calling the function for each item instead would run an insert per item and merge repeated keys, which the batch rejects.
- The tests of the core pass virtual time as the `$1` parameter of each statement, so the function would take the time as an argument, which every caller of SQL would see.

## The migration builds the function from the builders of the core

The migration could build the body from `insertedRunAt` and `mergeIntoPendingJob` of `sql/enqueue.ts`, with no copy to keep in sync.
A released migration never changes, but such a migration would change with the builders: a new schema would get the body of the current builders, and a schema migrated earlier the body of its release.
