# Testing

`kern-jobs/testing` gives an application a test double of the jobs API, `createTestJobs`, a run of a handler, `runJob`, and a run of `onFailure`, `runFailure`, without a database or a worker; `testing/test-jobs.ts`, `testing/run-job.ts`, and `testing/run-failure.ts`.

## Test double of the jobs API

- `createTestJobs` builds the jobs API with `createJobsApi` of `producer/jobs.ts` over a store in memory, as `createJobs` builds it over the SQL of the core, so that both check the arguments, the options, and the scope by the same code and reject with the same errors before the store records anything ([decision](../decisions/testing/in-memory-double.md)).
- The store serializes the payload of each enqueue by `encodePayload` of `sql/job.ts`, which the SQL enqueue calls, and discards the text, so that a payload for which `JSON.stringify` throws, such as a bigint or a circular object, rejects with its error, and if it returns `undefined`, the double records the original payload and the SQL enqueue stores SQL NULL.
  It serializes every payload of `enqueueMany` before it records any of them.
  It checks neither the constraints of the table, such as the length of a key or a queue, nor transactions: an enqueue in a transaction that rolls back stays recorded.
- Each enqueue, and each payload of `enqueueMany` in their order, is recorded as a call of its task, its scope (`null` for the empty scope), its payload as the code passed it, and its options.
  The options come from the parameters of the core: those that a layer gave, with `key: null` and `queue: null`, and without `maxWait` outside `debounce` and `resetAttempts` with `keep`, which `toEnqueueParams` drops; `toEnqueueParams` keeps the key mode absent when no layer gives it, so that the call tells it from an explicit `replace`.
- The store keeps the id, task, scope, and key of each pending job, and no payload or run time.
  An enqueue of the key of a pending job of its task and scope that takes new enqueues resolves to its id and `created: false` in every key mode; other enqueues add a pending job with the next id.
  `enqueueItems` is recorded as one call of its array, also an empty one, and resolves to the jobs that its items fill as a database fills them one enqueue after another, by the counts of the items of its pending jobs: the open batch of its key until it has `maxItems`, then new jobs, and an open batch of another `maxItems` is sealed; the double does not store the items.
  An enqueue that would merge into such a job of the other kind, a job that is not a batch into a batch or items into a job that is not a batch, rejects with an `Error` of the message and the `code` of the error of the database, rather than its `QueryError`, since the entry loads no database package; it rejects before anything is recorded, also for the payloads of `enqueueMany` before it.
  `delete` and `deletePending` remove the pending jobs of the scope that the jobs API resolved, of every scope for `null`; no job runs or fails, so `retry` resolves to `null` and `deleteFailed` to 0.
- Each call of `createTestJobs` has its own calls and pending jobs, so an instance with `ALL_SCOPES` does not reach those of another instance.
- `capture` adds a list to a set of captures for the time of its action, and each enqueue appends its call to every list in the set, so nested and concurrent captures each get every enqueue made while they run, and `clear` keeps the calls that a running capture holds.
- `callsFor` of a task selects the calls of its name in the scope that `getTaskScope` of `producer/jobs.ts` resolves at the query, as `deletePending({ task })` does: of a scoped task, the scope of `createTestJobs`; of another task, the empty scope, without calling the function of the scope, so that the calls of a scoped definition of the same name stay out.

## Run of a handler

- `runJob` gives the handler the context that the worker would give a job enqueued with the payload and no overrides, which `resolveJobInfo` resolves: the key, the queue, and `maxAttempts` of `getTaskEnqueueParams`, which also checks the options of the task, `DEFAULT_MAX_ATTEMPTS` of `sql/enqueue.ts` when they give none, the first attempt, and the id `"1"`; a field of `job` other than `undefined` replaces its default.
- The payload goes through `encodePayload` and `decodePayload` of `sql/job.ts`, as an enqueue stores it and the worker reads it, so a payload without JSON text, such as `undefined` or an object whose `toJSON` returns `undefined`, reaches the handler as `undefined`.
- A scoped task takes a scope, which the types require and `runJob` rejects with `TypeError` without, since the worker does not run such a job; a scope that `job` gives, of any task, passes `checkScope` of `limits.ts`.
- The arguments of a task that is not scoped are a union whose first member, with a payload, is not conditional on the payload type, so that a wrapper generic over the payload passes a payload and options.
- `runJob` reads each field of `options` and `options.job` at most once, since a getter may return another value on each read.
  It checks the scope and resolves the options of the task before it reads the other fields of `job`, except the key, which it reads before the options, whose checks take it as the key of an enqueue, so that the key of `job` replaces that of `withKey` without calling `withKey`, as an override of an enqueue does.
- `job.id` must have the form and the range of an id of the table, a decimal integer from 1 to 2^63 − 1 without leading zeros, though no row of it needs to exist: `resolveJobInfo` rejects another one by `checkJobId` of `producer/jobs.ts`, which `delete` and `retry` also run.
- `runJob` runs the handler by `runTaskHandler` of `task/run-handler.ts`, as the worker does, through `options.runHandler` when it is given, with the name of the task and the payload through JSON: it resolves or rejects as `runHandler` does, or as the handler does without it.
  It does not turn `RetryError`, `TerminalError`, and `BatchItemsError` into a settlement, and rejects with a `BatchItemsError` as it is, without the checks of the worker, which need the row of the job.
- It runs the `onFailure` of an implementation as the handler of its failure implementation, `implementation.failure`, with a `JobFailure` as the payload ([Failure jobs](failure-jobs.md#failure-tasks)).
- `testJobs.runJob` passes its options as they are to `runJobWithScope`, the body of `runJob`, so its job gets `options.job.scope` when it is not `undefined`.
  Otherwise its job gets the scope that an enqueue through the double gives, from `getEnqueueScope` of `producer/jobs.ts` at the call.
  A scoped task gets the scope of `createTestJobs` and rejects with the errors of an enqueue without one or with `ALL_SCOPES`.
  Another task gets `null`, and the function of the scope is not called.
  Its arguments are those of `runJob` for a task that is not scoped, which do not require `job.scope`.

## Run of `onFailure`

- `runFailure` runs the failure implementation of an implementation, `implementation.failure`, through `runJobWithScope`, with the `JobFailure` that it builds as its payload, so the failure goes through JSON, and the context is that of a failure job: its own id, the scope of the failed job, the first attempt, the `maxAttempts` of the failure task, and no key or queue.
  The id of the failure job is the one after the id of the failed job, since the identity of the table gives the failure job, inserted later, a greater id.
  It passes `options.runHandler` to `runJobWithScope`, so `runHandler` gets the name of the failure task.
- The payload of the failure goes through `encodePayload` and `decodePayload` as the root of its JSON, as an enqueue stores it, before the failure does, so a `toJSON` gets the key that an enqueue gives it, and a payload without JSON text leaves the failure without `payload`, as SQL NULL does.
- The types require an implementation with `failure`, and `runFailure` rejects one without it with `TypeError` before it reads the options.
- `job` of the failure is the metadata that `resolveJobInfo` gives a job of the payload, as `runJob` does, on its last attempt, `attempt` equal to its `maxAttempts`, with the task name of the implementation, since a failure for good other than by `TerminalError` happens on the last attempt.
  The scope of the failed job follows the rules of the scope of `runJob` and `testJobs.runJob`, and the failure job takes it.
- The other fields of the failure are the id `"1"`, the reason `handler-error`, and the current time as `failedAt` in the format of the SQL failure, by `toISOString`.
- The error is `{ message }` of `options.error`, since JSON drops the message of an `Error`, which is not enumerable, and by default the message that the worker or the reaper records for the reason, `FAILURE_MESSAGES` of `task/failure.ts`, which they share, or `job failed` for a reason that a thrown error causes.
- A field of `options` or `options.job` other than `undefined` replaces its default, and `runFailure` reads each of them at most once, the fields of `job` other than `scope` after the checks of the scope and the options of the task, but the key before the options, as `runJob` does.
