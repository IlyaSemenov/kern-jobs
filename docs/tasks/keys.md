# Deduplication and Debouncing

To keep at most one pending job per entity, such as one search indexing per product, give the jobs a key.
An enqueue with the key of a pending job of the same task and [scope](./scopes) merges into it instead of adding a job, by `keyMode`:

- `replace`, the default: the pending job takes the new payload and options, such as its delay.
- `keep`: the pending job takes the key mode but keeps its payload and other options, so the first event wins.
- `debounce`: the pending job takes the new payload and becomes due `delay` seconds after the latest enqueue, but no later than `maxWait` seconds after the first one.

`jobs.enqueue` returns the id of the pending job and whether it was `created` or merged.

Different tasks and different [scopes](./scopes) can use the same keys without affecting each other.
A key is at most 1024 bytes long.

## One pending job per scope

To merge the enqueues of a task in each [scope](./scopes) into one pending job, such as one job to recalculate the statistics of a tenant, give the task `key: true`:

```ts
export const recalculateStatsTask = defineTask("stats.recalculate", { scoped: true, key: true })
```

Passing `key: true` is equivalent to passing `key: ""`.
To cancel the pending job, call `jobs.deletePending({ task: recalculateStatsTask, key: true })`.

## Debounce

For example, to render the preview of a document once a series of edits ends, without postponing it for more than five minutes during a long series:

```ts
export const renderPreviewTask = defineTask("documents.renderPreview", {
  keyMode: "debounce",
  delay: 10,
  maxWait: 300,
})
  .withPayload<{ documentId: string }>()
  .withOptions(({ documentId }) => ({ key: `document:${documentId}` }))
```

`maxWait` limits only how long later enqueues postpone the job: like any job, it can start later, such as while the workers or its queue are busy, while a transaction that enqueued it stays open, or after its backoff when it fails.

These limits hold for a series whose enqueues pass the same `delay` and `maxWait`: an enqueue with a smaller `delay` or `maxWait` never makes the job due earlier.
To run the pending job sooner, enqueue it with `keyMode: "replace"`, which sets the run time of that enqueue.
An enqueue in another mode, `replace` or `keep`, ends the series, and the next `debounce` enqueue starts a new one.

## While a job of the key runs

A key merges only pending jobs: an enqueue while a job of the key runs adds the next pending job, which can start while the first one still runs.
To also run the jobs of an entity one at a time, give them a [queue](./queues) of the entity as well:

```ts
export const indexProductTask = defineTask("search.indexProduct", { delay: 5 })
  .withPayload<{ productId: string }>()
  .withOptions(({ productId }) => ({
    key: `product:${productId}`,
    queue: `product:${productId}`,
  }))
```

When the running job fails and has attempts left, or its worker [stops](../workers/stopping) and interrupts it, it merges with the pending job by the mode of the latest enqueue of the pending job:

- `replace` or `debounce`: the running job is deleted, and the pending job runs with the newer payload.
- `keep`: the pending job is deleted, and the running job runs again with its own payload, after its backoff when it failed, so the first event still wins.

The pending job stays as it is when the running job completes, fails for good, or its worker dies: the job of a dead worker that has attempts left runs again beside the pending job, without its key.

So treat a job of a key as a signal that its entity changed rather than as one event: every event either merges into a job that runs after it or becomes the next job, but several events run as one job with one payload, and [deleting](#cancel-a-pending-job) that job deletes them all.
Read the current state of the entity in the handler, and enqueue the events without a key to process each of them with its own payload.

## Cancel a pending job

To cancel the next run of a job of a key, such as a reminder whose reason is gone, delete its pending job:

```ts
export const remindAboutDraftTask = defineTask("documents.remindAboutDraft", {
  delay: 24 * 60 * 60,
})
  .withPayload<{ documentId: string }>()
  .withOptions(({ documentId }) => ({ key: `document:${documentId}` }))

// When the document is published.
await jobs.deletePending({ task: remindAboutDraftTask, key: `document:${documentId}` })
```

In one scope, `deletePending` resolves to `1` if it deleted the pending job of the key and to `0` otherwise; the job takes with it the events that merged into it.
For a [scoped](./scopes) task, it deletes the job of the scope of `jobs`, which an enqueue through `jobs` would merge into; for another task, it does not call the scope function of `jobs`.
Through `jobs` with [`ALL_SCOPES`](../workers/failed-jobs#jobs-of-a-scope), it deletes the job of the key of a scoped task in every scope, and resolves to their total.

Deleting the pending job neither interrupts a running attempt of a job of the key nor keeps it from being retried, so check in the handler whether its work is still needed.
