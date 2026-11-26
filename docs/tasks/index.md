# Defining Tasks

Define a task with `defineTask`, its name, and the type of its payload, and [enqueue](./enqueue) its jobs with `jobs.enqueue`:

```ts
import { defineTask } from "kern-jobs"

export const sendEmailTask = defineTask("email.send").withPayload<{ to: string; subject: string }>()

await jobs.enqueue(sendEmailTask, { to: "ann@example.com", subject: "Welcome" })
```

The name identifies the task in the database, so keep it unique within your application, and rename a task only [in steps](../operations/changing-tasks).
It is at most 128 bytes long, 120 for a task with an [`onFailure`](../workers/failure-handlers), and does not end with `:failure`, which names the failure jobs of a task.

The payload is stored as JSON, so pass values that survive `JSON.stringify`, such as an ISO string instead of a `Date`.

A task without a payload needs no `withPayload`:

```ts
export const purgeExpiredSessionsTask = defineTask("sessions.purgeExpired")

await jobs.enqueue(purgeExpiredSessionsTask)
```

To collect the items of many enqueues into one job, define a [batch task](./keys#batches) with `withBatchPayload` instead of `withPayload`, and enqueue its items with `jobs.enqueueItems`.

## Options of a task

To enqueue every job of a task the same way, pass the options to `defineTask`:

```ts
export const sendEmailTask = defineTask("email.send", { maxAttempts: 5 }).withPayload<{
  to: string
  subject: string
}>()
```

To compute the [key](./keys) from the payload, pass a function that returns it, or `null` for none, to `withKey`, and to compute other options, pass a function that returns them to `withOptions` after it:

```ts
export const indexProductTask = defineTask("search.indexProduct", { delay: 5 })
  .withPayload<{ productId: string; priority: boolean }>()
  .withKey(({ productId }) => `product:${productId}`)
  .withOptions(({ priority }) => ({ queue: priority ? "search:priority" : null }))
```

They replace the options of `defineTask` one by one, and the [overrides](./enqueue#overriding-options) of an enqueue replace them; `withKey` is not called for an enqueue that gives a key.
