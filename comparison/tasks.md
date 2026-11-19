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

## Running every handler in a context of the application

The worker takes one function, `runHandler`, that wraps the handler of every job, such as to run it in the context of the tenant of its scope, and a `runHandler` that resolves without calling the handler completes the job.

- Sidekiq, same: a server middleware, `call(job_instance, job_hash, queue)`, runs every job around its `yield`, and a middleware that returns without `yield` completes the job without `perform` ([source](https://github.com/sidekiq/sidekiq/blob/v8.1.7/lib/sidekiq/middleware/chain.rb#L51-L62), [source](https://github.com/sidekiq/sidekiq/blob/v8.1.7/lib/sidekiq/processor.rb#L189-L199)); a job gets no abort signal, and a shutdown raises `Sidekiq::Shutdown` in a job that still runs after its timeout ([source](https://github.com/sidekiq/sidekiq/blob/v8.1.7/lib/sidekiq/processor.rb#L51-L62)).
- Inngest, differs: `wrapFunctionHandler` of a middleware of the client or of a function wraps the handler, such as in an `AsyncLocalStorage` ([source](https://github.com/inngest/inngest-js/blob/inngest@4.26.2/packages/inngest/src/components/middleware/middleware.ts#L493-L507)), but it runs on every request of a run, which re-enters the handler for each step, and its JSDoc requires it to call `next()`.
- BullMQ, differs: a worker takes one processor for every job of its queue, which gets the job with its name and data and the abort signal ([source](https://github.com/taskforcesh/bullmq/blob/v6.7.5/src/types/processor.ts#L7-L12)), so the application dispatches by name and wraps the handlers in its processor.
- graphile-worker-tasks, differs: `wrapTask` of `wrapTaskDefinition` wraps the handler of each wrapped definition in the application, such as in the context of a tenant, and a wrapper that resolves without calling the task completes the job ([docs](https://github.com/IlyaSemenov/graphile-worker-tasks/blob/v2.6.1/README.md#wrapping-tasks)).
- Graphile Worker, lacks: the worker calls the task function directly ([source](https://github.com/graphile/worker/blob/v0.18.0/src/worker.ts#L248-L255)), and neither the hooks and middleware of its plugins nor its events wrap the run of a job ([source](https://github.com/graphile/worker/blob/v0.18.0/src/index.ts#L421-L557)), so the application wraps each task function.
- graphile-worker-orchid, lacks: its `makeWorkerUtils` has no hook around a handler, and its jobs run on the runner of Graphile Worker ([source](https://github.com/IlyaSemenov/graphile-worker-orchid/blob/v2.5.1/src/workerUtils.ts)).
- pg-boss, lacks: the application wraps each handler that it passes to `work` ([source](https://github.com/timgit/pg-boss/blob/12.38.1/src/manager.ts#L1331-L1333)).
