# Defining Tasks

Define a task with `defineTask`, its name, and the type of its payload, and enqueue its jobs with `jobs.enqueue`:

```ts
import { defineTask } from "kern-jobs"

export const sendEmailTask = defineTask("email.send").withPayload<{ to: string; subject: string }>()

await jobs.enqueue(sendEmailTask, { to: "ann@example.com", subject: "Welcome" })
```

The name identifies the task in the database, so keep it unique within your application, and rename a task only [in steps](../deployment/changing-tasks).
It is at most 128 bytes long.

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

They replace the options of `defineTask` one by one, as [overrides](#overriding-options) of an enqueue do.

## Delays

To run a job later, pass `delay` in seconds, or the time as `runAt`:

```ts
export const checkPaymentTask = defineTask("payments.check", { delay: 60 }).withPayload<{
  paymentId: string
}>()

export const sendReminderTask = defineTask("reminders.send")
  .withPayload<{ reminderId: string; at: string }>()
  .withOptions(({ at }) => ({ runAt: new Date(at) }))
```

A job with a `runAt` in the past runs as soon as a worker is free.

## Overriding options

To enqueue one job differently, pass options as the third argument of `jobs.enqueue`.
They replace the options of the task one by one:

```ts
await jobs.enqueue(indexProductTask, { productId }, { delay: 0 })
```

- An option passed as `undefined` keeps the option of the task.
- `key: null` and `queue: null` enqueue the job without the key or queue of the task.
- `delay` and `runAt` replace each other: passing one drops the other of the task.
