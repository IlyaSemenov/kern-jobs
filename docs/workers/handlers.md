# Writing Handlers

A handler gets the payload of its job and a context with the metadata of the job, such as the number of the attempt:

```ts [src/worker/tasks/render-preview.ts]
import { renderPreviewTask } from "../../tasks"

export default renderPreviewTask.implement(async ({ documentId }, { job }) => {
  console.log(`rendering ${documentId}, attempt ${job.attempt} of ${job.maxAttempts}`)
  await storePreview(documentId, await render(documentId))
})
```

Keep the implementations and the manifest in modules that only the worker imports, such as in `src/worker/`, so that the code that enqueues jobs loads the definitions of the tasks without the code of their handlers.

## When a job runs again

A job normally runs once: its handler returns, and the worker deletes the job.
A job runs again only after a failure or a stop:

- Its handler threw, and the job [retries](./retries): the retry runs the whole handler, also the steps that succeeded before the error.
- Its worker [stopped](./stopping) and aborted the handler, which threw, and the job returned to pending: the next run starts the handler from the beginning.
- Its worker died, stopped without waiting for the handler, or lost the database for longer than the [lease](./leases), after the handler did its work and before the worker recorded the completion.
  No setting rules it out.
- A worker with the [same `id`](./leases#return-the-jobs-of-a-restarted-worker-at-once) started while its worker had lost the connection that it keeps, and the job ran again before its handler ended.

After a failure or a stop, a job of a [key](../tasks/keys#while-a-job-of-the-key-runs) can instead merge with the pending job of its key, and the mode of the key decides which payload runs.

So decide for each handler what a second run does.
A handler that computes a result from the current state, such as one that indexes a product or renders a preview, can run again, but two of its runs can overlap or end out of order, such as the jobs of one [key](../tasks/keys), and the older run can then overwrite the result of the newer one.
Store such a result only if it comes from a newer version of the state than the stored one, and compare the versions in the statement that stores it, such as by a version column that every change of the entity increments.

For a step that must not repeat:

- A write to your database: in one transaction, first insert a record of the operation under a unique key, such as `payment:${paymentId}` from the payload, with `insert ... on conflict (key) do nothing returning 1`.
  Make the write in the same transaction only if the insert returned a row, so that two runs that overlap cannot both make it.
- A call of an external service, such as sending an email or charging a card: pass the service an idempotency key of the operation, such as `payment:${paymentId}` from the payload.
  Do not use `job.id` for it: a job keeps its id when an enqueue with its key replaces the payload between attempts, so the service would take the changed request for a repeat of the earlier one, and the ids of jobs repeat across databases.
  Without such a key, nothing rules out a repeat: a record written after the call is missing when the worker dies between them.
- Give the step a job of its own, so that an error of another step does not repeat it; this narrows the cases above and does not remove them.

## Stop long handlers

The context of a handler has an `AbortSignal`, which the worker aborts when the handler should stop.
Pass the signal to the calls that take one, such as `fetch`, and check it between the steps of a long handler:

```ts
export default renderPreviewTask.implement(async ({ documentId }, { signal }) => {
  for (const page of await loadPages(documentId)) {
    signal.throwIfAborted()
    await renderPage(page)
  }
})
```

The reason of the signal is a `HandlerAbort`, whose `reason` tells why:

- `stop`: the worker stops, and the handler did not end in time.
- `lost`: the worker no longer holds the job, and another worker may run it.
- `deadline`: the worker could not extend the lease, which is about to end.

After the abort of a stop, a handler that throws while the stop still waits for it returns its job to pending at once without spending an attempt, also on its last attempt, and `onError` does not get the error; a `TerminalError`, a `RetryError`, or a return keeps its usual meaning.
Once the stop [stops waiting](./stopping#stop-from-your-code), at its timeout or when forced, the outcome of a handler that ends later does not change its job, which returns later with its attempt spent, or fails if that was its last attempt.

A handler that ignores the signal keeps running: it delays the stop of its worker, and after `lost` or `deadline`, another worker can run the same job at the same time.

## Wrap every handler

To run every handler of a worker inside code of your own, such as to set the current tenant, a logging context, or a tracing span, or to skip the handler of a job, pass `runHandler` to `startWorker`.
It gets the context of the handler with the name of the task and the payload of the job, and `run`, which runs the handler and returns its promise:

```ts [src/worker/start.ts]
import { logContext } from "../logger"

const worker = startWorker(driver, {
  manifest,
  runHandler: ({ task, job }, run) => logContext.run({ task, jobId: job.id }, run),
})
```

The worker takes the outcome of `runHandler` as the outcome of the handler, also after [an abort](#stop-long-handlers), so call `run` at most once and return or await its promise.
A `runHandler` that resolves without calling `run` completes the job without running its handler, such as the job of a [tenant](../tasks/scopes) that no longer exists.

To test a `runHandler`, pass the one that you pass to `startWorker` to [`runJob`](../testing/#run-a-handler) of `kern-jobs/testing` in `runHandler`.
`runJob` runs the handler through it with the context that a worker gives, and resolves to what `runHandler` resolves to, or rejects with what it throws.
To check whether `runHandler` ran the handler, replace the handler of the implementation with a mock:

```ts [src/worker/run-handler.test.ts]
import { expect, mock, test } from "bun:test"
import { runJob } from "kern-jobs/testing"

import { runHandler } from "./run-handler"
import buildReport from "./tasks/build-report"

test("skips the job of a deleted tenant", async () => {
  const handler = mock(() => {})
  await runJob(
    { ...buildReport, handler },
    { reportId: "report-1" },
    { job: { scope: "deleted-tenant" }, runHandler },
  )
  expect(handler).not.toHaveBeenCalled()
})
```

[`runFailure`](./failure-handlers#test-onfailure), `testJobs.runJob`, and `testJobs.runFailure` take `runHandler` too; `runFailure` runs `onFailure` through it with the name of the failure task, such as `reports.build:failure`.
