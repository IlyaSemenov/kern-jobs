# Architecture

kern-jobs is a background job library on PostgreSQL for TypeScript.
Jobs live in one table, and producers enqueue them with the connection of the application, so a job enqueued in a transaction becomes visible to workers when that transaction commits.

Its core promises:

- A key keeps at most one pending job per scope and task, and no event of the key is lost unless the application deletes the pending job that carries it: an event either merges into the pending job by the mode of the key, and that job runs after the event commits, or becomes the next pending job, also while a job of the key runs.
- A queue runs at most one job at a time per scope and starts the earliest of its ready jobs of the tasks of the claiming worker in the snapshot of each claim.
- A worker owns a job through a claim, which every heartbeat and settlement proves; a job whose lease expires, or whose worker has ended when a worker of its id starts, returns to pending, or fails on its last attempt, and a stale claim can neither extend nor settle it.
- A terminal failure of a claim whose implementation has `onFailure` inserts exactly one job that runs it, of the failure task of its task, in the statement that records the failure, also when the reaper records it.
- Deadlines are computed in SQL from the committed row, so concurrent producers and workers never move a lease or the debounce deadline of a window backwards, and no enqueue moves a retry before the end of its backoff.

The public API is not settled before 1.0 and may change in any release.
This directory specifies the SQL core, the producer API, and the worker runtime: the data model, operations, invariants, and accepted limitations.

[Decisions](../decisions/README.md) record the trade-offs between designs, with their alternatives and costs, and the contracts here link them.

## Terms

- An installation is one job table, which `migrateJobs` creates in a PostgreSQL schema of its own, with the producers and workers that use it; its schema tells it apart from another installation in the same database.
- A task is a kind of work defined in code, such as sending an email; a job is one row of the table that a worker executes as a task.
- A scope is a namespace of keys and queues, such as of a tenant: a key merges and a queue serializes only the jobs of its scope.
  It separates jobs logically within one table and one set of workers; it is neither a partition of the table, nor a quota, nor access control.
  An enqueue of the jobs API of a task defined with `scoped: true` takes the scope from the producer, and of another task gives its job the empty scope; the [SQL function](enqueue.md#sql-function) takes the scope of its arguments.
  An unscoped job is a job whose scope is empty.
- A key identifies the pending job of a task within a scope, such as `product:42`.
- A queue serializes the jobs of a scope that name it.
- A claim is the ownership of an active job by a worker, identified by a claim id.
- A lease is the time until which the reaper leaves a claimed job to its worker.
- Settlement ends a claim: complete deletes the job, and fail retries it, merges it with the pending job of its key, or records it as failed.
- The failure task of a task runs the `onFailure` of its implementation and has the name of the task with `:failure` after it; a failure job is the job of that task that a terminal failure of a claim of the implementation inserts.
- Administration is what the application does with existing jobs: it retries a failed job, and deletes pending or failed jobs by id or by a filter.
- Storable text is a string that has neither U+0000 nor a lone surrogate.
  PostgreSQL rejects U+0000 in text, and encoding as UTF-8 replaces a lone surrogate with U+FFFD, so two different strings would become one value, such as two keys that share a pending job.
  The API rejects a name, scope, key other than `true`, queue, or ID of the application that is not storable text before a statement uses it.

## Entry points

- `kern-jobs`, `index.ts`, gives what producers and workers share: the definitions of tasks and the enqueue.
- `kern-jobs/testing`, `testing.ts`, gives what tests of an application run without a database or a worker: `createTestJobs`, `runJob`, and `runFailure`.
- `kern-jobs/migrations`, `migrations.ts`, gives what creates and updates the schema before producers and workers start: `migrateJobs`.
- `kern-jobs/worker`, `worker.ts`, gives what only a worker process runs: `startWorker`, `waitForSignalAndStop` and `stopOnSignals`, the manifest and its check, the errors of handlers, the reason of an abort, and the type of a driver.
- `kern-jobs/drivers/postgres-js` and `kern-jobs/drivers/node-postgres`, `drivers/postgres-js.ts` and `drivers/node-postgres.ts`, give the driver of kern-db over each client, which the worker and `migrateJobs` take.

Importing `kern-jobs` or `kern-jobs/testing` loads no database package (kern-db, kern-orm, or kern-migrations) or client library: these entries do not import them at runtime, and the statements of the main entry build their objects without `createStatement` of kern-db.
The package test enqueues through the main entry, and records an enqueue and runs a handler through `kern-jobs/testing`, in a project that has none of them.

## Database access

The SQL core spans three directories under `src/`.

- `producer/` implements the jobs API and runs retry and deletion through the application's database client.
- `worker/db/` implements claims, leases, settlement, failure-job insertion, and schedule evaluation on the worker's database.
- `sql/` contains the shared job model, PostgreSQL helpers for building and executing statements, and the enqueue implementation used by both the jobs API and worker schedules.

Modules of `sql/` other than tests must not import kern-db or kern-orm, not even their types, since the main entry imports them.

## Map

Module paths are relative to `src/`.

- [Data model](data-model.md): the table, its states, indexes, and invariants, and the summary view; `sql/job-table.ts`, `sql/job.ts`, `migration/migrations/0003_job_summary.ts`, and `migration/migrations/0004_failure_jobs.ts`.
- [Migrations](migrations.md): the history of the migrations of the schema, `migrateJobs`, and the check of a schema by a worker; `migration/`.
- [Enqueue](enqueue.md): key modes, debounce, and the SQL function `enqueue`; `sql/enqueue.ts` and `migration/migrations/0005_enqueue_function.ts`.
- [Producer](producer.md): task definitions, enqueue options, and the database client of the application; `task/task.ts`, `task/options.ts`, `task/delay.ts`, `limits.ts`, and `producer/`.
- [Schedules](schedules.md): UTC calendars, persistent cursors, and atomic enqueue; `schedule/` and `worker/db/schedule.ts`.
- [Administration](administration.md): retry of failed jobs, also by the SQL function `retry`, and delete of pending or failed jobs by id or by a filter, by the application; `producer/administration.ts` and `migration/migrations/0006_retry_function.ts`.
- [Claiming](claiming.md): claiming jobs with and without queues, and the claim cycle; `worker/db/claim.ts`.
- [Settlement](settlement.md): complete, fail, and connection loss in a transaction; `worker/db/settlement.ts`.
- [Failure jobs](failure-jobs.md): the failure task that `implement` makes of `onFailure`, the stamp of each claim, and the failure job that a terminal failure inserts with its `JobFailure`; `worker/db/failure.ts`, `task/failure.ts`, `createFailureImplementation` and `resolveFailure` of `task/task.ts`, and `toManifestTasks` of `task/manifest.ts`.
- [Leases](leases.md): heartbeat, the reaper, and fencing; `worker/db/lease.ts`.
- [PostgreSQL](postgresql.md): supported versions, isolation, transactions, and time; `sql/postgres.ts`.
- [Drivers](drivers.md): the drivers of kern-db that the worker relies on, the statements of the core, the connections of the worker, and transactions; `drivers/`, `worker/driver.ts`, `worker/db/db.ts`, `worker/db/base.ts`, `worker/db/types.ts`, `worker/db/tables/`, `sql/statement.ts`, `worker/db/sql.ts`, and `worker/db/violation.ts`.
- [Testing](testing.md): the test double of the jobs API and the runs of a handler and of `onFailure` in a test; `testing/`.
- [Worker](worker.md): the worker of an application and its runtime: options, implementations of tasks and the manifest, handler errors and backoff, the check of the schema, slots, claiming, leases, settlement, the reaper, stopping, and errors; `worker/`, `task/manifest.ts`, `task/run-handler.ts`, `task/errors.ts`, and `task/backoff.ts`.
