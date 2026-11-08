# Tasks

## Payload types shared by producers and workers

A definition of a task, which producers import without its handler, types the payload of both an enqueue and the handler.

- graphile-worker-tasks, same: `defineTask(name).withPayload<T>()` defines a task without its implementation ([docs](https://github.com/IlyaSemenov/graphile-worker-tasks/blob/v2.6.1/README.md#use)).
- Graphile Worker, differs: an application declares the payload types of its tasks in the global interface `GraphileWorker.Tasks` by declaration merging ([docs](https://github.com/graphile/worker/blob/v0.18.0/website/docs/typescript.md#assuming-type-via-graphileworkertasks)).
- pg-boss, lacks: the type of the data that a handler of `work` receives was not connected to the data passed to `send` ([source](https://github.com/timgit/pg-boss/blob/12.38.1/src/manager.ts#L1331-L1332), [pg-boss#692](https://github.com/timgit/pg-boss/issues/692)), and typed task definitions shared by producers and workers were proposed for it ([pg-boss#707](https://github.com/timgit/pg-boss/pull/707#issuecomment-6218143914)).

## Terminal errors and retry delays from a handler

A handler fails its job for good by throwing `TerminalError`, and retries it after a given delay by throwing `RetryError`.

- Inngest, same: `NonRetriableError` and `RetryAfterError` ([source](https://github.com/inngest/inngest-js/blob/inngest%404.26.2/packages/inngest/src/components/NonRetriableError.ts), [source](https://github.com/inngest/inngest-js/blob/inngest%404.26.2/packages/inngest/src/components/RetryAfterError.ts)).
- BullMQ, differs: `UnrecoverableError` fails a job for good ([source](https://github.com/taskforcesh/bullmq/blob/v6.7.5/src/classes/errors/unrecoverable-error.ts)), and a backoff strategy in the settings of the worker computes the delay of a retry from the error ([source](https://github.com/taskforcesh/bullmq/blob/v6.7.5/src/types/backoff-strategy.ts)).
- Sidekiq, differs: `sidekiq_retry_in` of a job class takes the exception and returns the delay of the retry ([source](https://github.com/sidekiq/sidekiq/blob/v8.1.7/lib/sidekiq/job_retry.rb#L219-L231)).
