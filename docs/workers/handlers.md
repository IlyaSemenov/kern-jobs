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

- Its handler threw: the [retry](./retries) runs the whole handler, also the steps that succeeded before the error.
- Its worker [stopped](./stopping) and aborted the handler, which threw: the job runs again from the start.
- Its worker died, or lost the database for longer than the [lease](./leases), after the handler did its work and before the worker recorded the completion.
  No setting rules it out.

So decide for each handler what a second run does.
A handler that computes a result from the current state, such as one that indexes a product or renders a preview, can run again, but two of its runs can overlap or end out of order, such as the jobs of one [key](../tasks/keys), and the older run can then overwrite the result of the newer one.
Store such a result only if it comes from a newer version of the state than the stored one, and compare the versions in the statement that stores it, such as by a version column that every change of the entity increments.

For a step that must not repeat:

- A write to your database: commit it in one transaction with a record that the job did it, and check the record first.
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

A handler that ignores the signal keeps running: it delays the stop of its worker, and after `lost` or `deadline`, another worker can run the same job at the same time.

After `stop`, a handler that throws returns its job to pending at once without spending an attempt, also on its last attempt, and `onError` does not get the error.
A `TerminalError` or a `RetryError` keeps its meaning also after the abort, and a handler that returns completes its job.
