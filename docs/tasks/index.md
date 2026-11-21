# Defining Tasks

Define a task with `defineTask`, its name, and the type of its payload, and [enqueue](./enqueue) its jobs with `jobs.enqueue`:

```ts
import { defineTask } from "kern-jobs"

export const sendEmailTask = defineTask("email.send").withPayload<{ to: string; subject: string }>()

await jobs.enqueue(sendEmailTask, { to: "ann@example.com", subject: "Welcome" })
```

The name identifies the task in the database, so keep it unique within your application, and rename a task only [in steps](../deployment/changing-tasks).
It is at most 128 bytes long, 120 for a task with an [`onFailure`](../workers/failure-handlers), and does not end with `:failure`, which names the failure jobs of a task.

The payload is stored as JSON, so pass values that survive `JSON.stringify`, such as an ISO string instead of a `Date`.

A task without a payload needs no `withPayload`:

```ts
export const purgeExpiredSessionsTask = defineTask("sessions.purgeExpired")

await jobs.enqueue(purgeExpiredSessionsTask)
```

## Options of a task

To enqueue every job of a task the same way, pass the options to `defineTask`:

```ts
export const sendEmailTask = defineTask("email.send", { maxAttempts: 5 }).withPayload<{
  to: string
  subject: string
}>()
```

To compute options from the payload, such as a key, pass a function that returns them to `withOptions`:

```ts
export const indexProductTask = defineTask("search.indexProduct", { delay: 5 })
  .withPayload<{ productId: string }>()
  .withOptions(({ productId }) => ({ key: `product:${productId}` }))
```

They replace the options of `defineTask` one by one, as [overrides](./enqueue#overriding-options) of an enqueue do.
