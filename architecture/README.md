# Architecture

kern-jobs is a background job library on PostgreSQL for TypeScript.
Jobs live in one table, and producers enqueue them with the connection of the application, so a job enqueued in a transaction becomes visible to workers when that transaction commits.

Its core promises:

- A key keeps at most one pending job per scope and task, and no event of the key is lost unless the application deletes the pending job that carries it: an event either merges into the pending job by the mode of the key, and that job runs after the event commits, or becomes the next pending job, also while a job of the key runs.
- A queue runs at most one job at a time per scope and starts the earliest of its ready jobs of the tasks of the claiming worker in the snapshot of each claim.
- A worker owns a job through a claim, which every heartbeat and settlement proves; a job whose lease expires returns to pending, or fails on its last attempt, and a stale claim can neither extend nor settle it.
- Deadlines are computed in SQL from the committed row, so concurrent producers and workers never move a lease or the debounce deadline of a window backwards, and no enqueue moves a retry before the end of its backoff.

The public API is not settled before 1.0 and may change in any release.
This directory specifies the SQL core, the producer API, and the worker runtime: the data model, operations, invariants, and accepted limitations.

[Decisions](../decisions/README.md) record the trade-offs between designs, with their alternatives and costs, and the contracts here link them.

## Terms

- A task is a kind of work defined in code, such as sending an email; a job is one row of the table that a worker executes as a task.
- A scope is a namespace of keys and queues, such as of a tenant: a key merges and a queue serializes only the jobs of its scope.
  It separates jobs logically within one table and one set of workers; it is neither a partition of the table, nor a quota, nor access control.
  The jobs of a task defined with `scoped: true` take their scope from the producer, and the jobs of other tasks have the empty scope.
- A key identifies the pending job of a task within a scope, such as `product:42`.
- A queue serializes the jobs of a scope that name it.
- A claim is the ownership of an active job by a worker, identified by a claim id.
- A lease is the time until which the reaper leaves a claimed job to its worker.
- Settlement ends a claim: complete deletes the job, and fail retries it, merges it with the pending job of its key, or records it as failed.
- Administration is what the application does with jobs by their ids: it retries a failed job and deletes a pending or failed one.

## Entry points

- `kern-jobs`, `index.ts`, gives what producers and workers share: the definitions of tasks and the enqueue.
- `kern-jobs/migrations`, `migrations.ts`, gives what creates and updates the schema before producers and workers start: `migrateJobs`.
- `kern-jobs/worker`, `worker.ts`, gives what only a worker process runs: `startWorker`, `stopOnSignals`, the manifest and its check, the errors of handlers, the reason of an abort, and the type of a driver.
- `kern-jobs/db/postgres-js` and `kern-jobs/db/node-postgres`, `db/postgres-js.ts` and `db/node-postgres.ts`, give the driver of kern-db over each client, which the worker and `migrateJobs` take.

Importing `kern-jobs` loads neither kern-db, kern-migrations, nor a client library: the main entry does not import them at runtime, and its statements build their objects without `createStatement` of kern-db.
The package test enqueues through the main entry in a project that has none of them.

## Map

Module paths are relative to `src/`.

- [Data model](data-model.md): the table, its states, indexes, and invariants; `core/schema.ts` and `core/job.ts`.
- [Migrations](migrations.md): the history of the migrations of the schema, `migrateJobs`, and the check of a schema by a worker; `migration/`.
- [Enqueue](enqueue.md): key modes and debounce; `core/enqueue.ts`.
- [Producer](producer.md): task definitions, enqueue options, and the database client of the application; `task/task.ts`, `task/options.ts`, `task/delay.ts`, and `jobs/`.
- [Administration](administration.md): retry of failed jobs and delete of pending or failed ones by the application; `core/administration.ts`.
- [Claiming](claiming.md): claiming jobs with and without queues, and the claim cycle; `core/claim.ts`.
- [Settlement](settlement.md): complete, fail, and connection loss in a transaction; `core/settlement.ts`.
- [Leases](leases.md): heartbeat, the reaper, and fencing; `core/lease.ts`.
- [PostgreSQL](postgresql.md): supported versions, isolation, transactions, and time; `core/db.ts`.
- [Drivers](drivers.md): the drivers of kern-db that the worker relies on, the statements of the core, the connections of the worker, and transactions; `db/`, `core/driver.ts`, `core/statement.ts`, and `core/violation.ts`.
- [Worker](worker.md): the worker of an application and its runtime: options, implementations of tasks and the manifest, handler errors and backoff, the check of the schema, slots, claiming, leases, settlement, the reaper, stopping, and errors; `worker/`, `task/manifest.ts`, `task/errors.ts`, and `task/backoff.ts`.
