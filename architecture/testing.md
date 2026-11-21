# Testing

`kern-jobs/testing` gives an application a test double of the jobs API, `createTestJobs`, and a run of a handler, `runJob`, without a database or a worker; `testing/test-jobs.ts` and `testing/run-job.ts`.

## Test double of the jobs API

- `createTestJobs` builds the jobs API with `createJobsApi` of `jobs/jobs.ts` over a store in memory, as `createJobs` builds it over the SQL of the core, so that both check the arguments, the options, and the scope by the same code and reject with the same errors before the store records anything ([decision](../decisions/testing/in-memory-double.md)).
- The store serializes the payload of each enqueue by `encodePayload` of `core/job.ts`, which the SQL enqueue calls, and discards the text, so that a payload for which `JSON.stringify` throws, such as a bigint or a circular object, rejects with its error, and if it returns `undefined`, the double records the original payload and the SQL enqueue stores SQL NULL.
  It checks neither the constraints of the table, such as the length of a key or a queue, nor transactions: an enqueue in a transaction that rolls back stays recorded.
- Each enqueue is recorded as a call of its task, its scope (`null` for the empty scope), its payload as the code passed it, and its options.
  The options come from the parameters of the core: those that a layer gave, with `key: null` and `queue: null`, and without `maxWait` outside `debounce` and `resetAttempts` with `keep`, which `toEnqueueParams` drops; `toEnqueueParams` keeps the key mode absent when no layer gives it, so that the call tells it from an explicit `replace`.
- The store keeps the id, task, scope, and key of each pending job, and no payload or run time.
  An enqueue of the key of a pending job of its task and scope resolves to its id and `created: false` in every key mode; other enqueues add a pending job with the next id.
  `delete` and `deletePending` remove the pending jobs of the scope that the jobs API resolved, of every scope for `null`; no job runs or fails, so `retry` resolves to `null` and `deleteFailed` to 0.
- Each call of `createTestJobs` has its own calls and pending jobs, so an instance with `ALL_SCOPES` does not reach those of another instance.
- `capture` adds a list to a set of captures for the time of its action, and each enqueue appends its call to every list in the set, so nested and concurrent captures each get every enqueue made while they run, and `clear` keeps the calls that a running capture holds.
- `callsFor` of a task selects the calls of its name in the scope that `getTaskScope` of `jobs/jobs.ts` resolves at the query, as `deletePending({ task })` does: of a scoped task, the scope of `createTestJobs`; of another task, the empty scope, without calling the function of the scope, so that the calls of a scoped definition of the same name stay out.

## Run of a handler

- `runJob` gives the handler the context that the worker would give a job enqueued with the payload and no overrides: the key, the queue, and `maxAttempts` of `getTaskEnqueueParams`, which also checks the options of the task, `DEFAULT_MAX_ATTEMPTS` of `core/enqueue.ts` when they give none, the first attempt, and the id `"1"`; a field of `job` other than `undefined` replaces its default.
- The payload goes through `encodePayload` and `decodePayload` of `core/job.ts`, as an enqueue stores it and the worker reads it, so a payload without JSON text, such as `undefined` or an object whose `toJSON` returns `undefined`, reaches the handler as `undefined`.
- A scoped task takes a scope, which the types require and `runJob` rejects with `TypeError` without, since the worker does not run such a job; a scope that `job` gives, of any task, passes `checkScope` of `jobs/jobs.ts`.
- `runJob` does not run the `runHandler` of a worker, nor turn `RetryError` and `TerminalError` into a settlement: it resolves or rejects as the handler does.
- It runs the `onFailure` of an implementation as the handler of its failure implementation, `implementation.failure`, with a `JobFailure` as the payload ([Failure jobs](failure-jobs.md#failure-tasks)).
