# kern-jobs

## 0.4.0

### Minor Changes

- dcdc055: Add `runFailure` and `testJobs.runFailure` to `kern-jobs/testing`, which run the `onFailure` of a task implementation with a `JobFailure` built from the payload of the failed job.
- 4e25ac9: Add the `runHandler` option of a worker to `runJob`, `runFailure`, `testJobs.runJob`, and `testJobs.runFailure`, and export the `RunHandler` and `RunHandlerContext` types from `kern-jobs/testing`.
- 96326a8: Add the SQL function `retry`, which retries a failed job by its id in any scope, such as in `psql`.
- 75d0b64: Add `testJobs.runJob`, which runs a handler as `runJob` does and gives the job of a scoped task the scope of `createTestJobs`.

### Patch Changes

- dcdc055: Fix `runJob` and `testJobs.runJob` accepting a `job.id` that is not the id of a job, such as `"job-1"`.

## 0.3.0

### Minor Changes

- 5641119: `retry`, `delete`, and `deletePending` without a filter, through `jobs` created without a scope, act only on unscoped jobs rather than on the jobs of every scope.
  Pass `scope: ALL_SCOPES` to `createJobs` to act on the jobs of every scope.
- 74401d7: Add `jobs.deleteFailed`, which deletes failed jobs in bulk, filtered by task, by age, or both, with the scope rules of `jobs.deletePending`, without looking up their ids, and resolves to their count.
- 7a1b27b: Add `jobs.deletePending`, which deletes the pending jobs of the scope of `jobs`, of a task, or of a key of a task without looking up their ids, and resolves to their count.
- cda9de7: Rename the driver entry points `kern-jobs/db/postgres-js` and `kern-jobs/db/node-postgres` to `kern-jobs/drivers/postgres-js` and `kern-jobs/drivers/node-postgres`.
- 632fd5e: Add `jobs.enqueueMany`, which enqueues many payloads of one task atomically.
- b125795: Add `decision` to the `handler` error reports of `onError`: the decision of the worker to retry the job or to fail it for good.
- eba7486: Add the `job_summary` view for queue metrics.
- 2aeb8e1: Use kern-orm instead of kern-db for database access.
- dc383f8: Accept `key: true` as the empty key in the options of an enqueue and the filter of `deletePending`, so that the enqueues of a task merge into one pending job per scope without a constant key.
- e233820: Add `mergeManifests` to run the tasks and schedules of several manifests in one worker.
- d1da46d: Name the table of the applied migrations of kern-jobs `migration`.
- 1a8d18d: Add `onFailure` to `task.implement`, which handles a job that fails for good in a retryable job of its own; task names ending in `:failure` are reserved for these jobs.
- a52428e: Add the `runHandler` option of `startWorker`, which wraps or skips the handler of every job, such as to run it in the context of the tenant of its scope.
- c27517e: Enqueue jobs from SQL, such as in migrations and backfills of the application, with the function `enqueue` in the schema of kern-jobs.
- 86c3e24: Start each SQL statement of kern-jobs with a comment that names it, such as `/* kern-jobs:claim:plain */`, to find it in PostgreSQL activity and logs.
- 2662d3c: Add `force: true` to `worker.stop`, which aborts the handlers and rejects at once, also when another call has begun the stop; every call of `stop` now checks its options.
- 2997511: Add `kern-jobs/testing` with `createTestJobs`, a jobs API for tests that records enqueues in memory, and `runJob`, which calls the handler of a task directly with its payload through JSON and the metadata of its job.
- e1f51f0: Add `waitForSignalAndStop`, which stops a worker at SIGTERM or SIGINT and returns control to the application instead of exiting the process.
- ed3b254: A worker that starts with the `id` of a worker that died returns its jobs at once, without waiting for their leases.

### Patch Changes

- 75e453a: Fix `createJobs` calling a callable executor as a client factory instead of running statements through its `query` method.

## 0.2.0

### Minor Changes

- dc58686: Add `defineSchedule` to enqueue a task on a UTC cron calendar from a worker manifest, with optional catch-up after downtime.

### Patch Changes

- f1d6d29: Validate task names, scopes, keys, queues, worker IDs, and schema names for characters that PostgreSQL cannot store as given.

## 0.1.0

### Minor Changes

- d129a69: Initial release.
