# Terminal failure hook

A hook of a task that runs once its job fails for good, so that the application marks its own row failed, such as a chat message that waits for a reply.

## Use case

An application had three handlers that, on an error with `job.attempt >= job.maxAttempts`, marked an entity of the application failed before they rethrew the error.
The check is wrong in three cases:

- `stop` interrupts the last attempt: the handler has marked its entity failed, and the job returns to pending with its attempt given back and runs again.
  A check of the signal in the handler does not help, since the worker decides on the interruption once the handler ends, and `stop` can abort the signal while the handler marks its entity.
- The worker crashes or loses the lease on the last attempt, or the deadline of its lease passes before the handler starts: the job fails for good without the check, and the entity waits forever.
- A `TerminalError` fails the job for good on an earlier attempt.

A handler that throws a `TerminalError` with the error as its cause after the mark covers the first case, since the worker fails the job for good on a `TerminalError` also after `stop`.
A flag of the job, such as `job.isFinalAttempt`, covers none of them, since the outcome is known only after the handler ends.

## Preliminary

- The hook runs at least once for each job that fails for good, including one that the reaper fails or that a worker fails without running its handler, so a worker of the task runs it as work of its own rather than the process that fails the job.
- The hook gets the payload, the metadata of the job, and the last error.

## Open

- How the fail statements of settlement and of the reaper learn that a task has the hook: from a column that the enqueue sets from the definition of the task, or by workers claiming the failed jobs of the tasks whose implementations have the hook, which runs a hook added later on old failed jobs.
- Whether the hook runs as a job of its own, such as of another task, or as a state of the failed row, and how it retries.
- Whether a hook runs in the transaction that fails the job instead, after step 2 of [Fail](../architecture/settlement.md#fail), so that an error of the hook rolls the fail back; the reaper fails jobs without the code of their tasks, so such a hook would not run for them.
- Whether the hook runs again when the application retries a failed job that then fails for good again.

## Other projects

- Sidekiq, has: the `sidekiq_retries_exhausted` block of a job class runs in the process once the last retry fails, and an error caused by the shutdown does not reach it ([source](https://github.com/sidekiq/sidekiq/blob/v8.1.7/lib/sidekiq/job_retry.rb#L117-L138), [source](https://github.com/sidekiq/sidekiq/blob/v8.1.7/lib/sidekiq/job_retry.rb#L250-L273)).
- pg-boss, has: the `deadLetter` queue of a queue gets a copy of the payload of a job that fails after all retries, with the id, the retry count, and the output of the failed job ([docs](https://github.com/timgit/pg-boss/blob/12.38.1/docs/api/queues.md#L69-L71)); the statement that fails a job, also one that expires it, inserts the copy ([source](https://github.com/timgit/pg-boss/blob/12.38.1/src/plans.ts#L3288-L3296), [source](https://github.com/timgit/pg-boss/blob/12.38.1/src/plans.ts#L3532-L3562)).
- Inngest, has: `onFailure` of a function registers a separate function that the event `inngest/function.failed` of the function triggers once it runs out of retries ([source](https://github.com/inngest/inngest-js/blob/inngest%404.26.2/packages/inngest/src/components/InngestFunction.ts#L226-L253), [source](https://github.com/inngest/inngest-js/blob/inngest%404.26.2/packages/inngest/src/components/InngestFunction.ts#L697-L707)).
