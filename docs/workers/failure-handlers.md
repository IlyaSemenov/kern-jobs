# Failure Handlers

To act when a job fails for good, such as to flag an order for review or send a notification, pass `onFailure` to the implementation of its task:

```ts [src/tasks.ts]
import { defineTask } from "kern-jobs"

export interface ChargeOrder {
  orderId: string
  /** Version of the charge of the order: starting the charge again increments it, and a retry of a job does not. */
  generation: number
}

export const chargeOrderTask = defineTask("orders.charge").withPayload<ChargeOrder>()
```

```ts [src/worker/tasks/charge-order.ts]
import { chargeOrderTask } from "../../tasks"

export default chargeOrderTask.implement(
  async ({ orderId, generation }) => {
    await chargeOrder(orderId, generation)
  },
  {
    onFailure: async failure => {
      const { orderId, generation } = failure.payload
      await sql`
        -- The payment may have gone through before the worker died.
        update orders set status = 'charge_review', failure_reason = ${failure.error.message}
        where id = ${orderId} and charge_generation = ${generation} and status = 'charging'
      `
    },
  },
)
```

`onFailure` runs in a job of its own, the failure job, which kern-jobs adds in the transaction that records the failure, also when the worker died on the last attempt.
The failure job [retries](./retries) as any job.

A job fails for good by a [`TerminalError`](./retries#decide-in-the-handler) or on its last attempt.
A job that returns to pending adds no failure job, also when a [stop](./stopping) interrupted its last attempt.
The failed job stays [failed](./failed-jobs), whatever `onFailure` does.

## What `onFailure` gets

Its first argument is the `JobFailure` of the failed job, with its `payload` as stored when it failed, its metadata as `job`, and:

- `id`: the id of the failure, the same in each run of `onFailure` for it.
- `reason`: why the failure was recorded, such as `handler-error` or `lease-expired`.
  Under any reason, the handler may already have done its work.
- `error.message`: the message that the table stores in `last_error`, such as `lease expired`.
  The error itself, with its class and stack, reaches only [`onError`](./monitoring#report-errors) of the worker that ran the handler.

Its second argument is the [context](./handlers) of a handler, whose `job` is the failure job itself.

## The failure job

The failure job has the name of your task with `:failure` after it, such as `orders.charge:failure`: the [summary of the jobs](./monitoring#watch-the-backlog), [`runHandler`](./handlers#wrap-every-handler), and [`onError`](./monitoring#report-errors) show it by this name.
It has the scope of the failed job, which `onFailure` of a [scoped](../tasks/scopes) task gets as `job.scope` of its context, and no key or queue, so it does not wait for the other jobs of the key or the queue of the failed job.

It runs under the [lease](./leases) of the worker, and by default it has 25 attempts with the default [backoff](./retries#backoff), which last about 14 hours.
To outlast a longer outage of a service that `onFailure` needs, give it as an object of the handler with `maxAttempts`, `backoff`, or both:

```ts
export default chargeOrderTask.implement(handler, {
  onFailure: {
    // About 4 days with the default backoff.
    maxAttempts: 100,
    handler: async failure => {
      // ...
    },
  },
})
```

Watch the failed jobs of the names that end with `:failure`: a failure job that spends its attempts stays failed, until you [retry](./failed-jobs#retry-a-failed-job) it.
To [delete](./failed-jobs#delete-failed-jobs) the failure jobs of a task, pass the `failure.task` of its implementation as the task.

## Write an `onFailure` that can run twice

An `onFailure` can [run more than once](./handlers#when-a-job-runs-again), and it can run after the operation that failed has been started again and has succeeded.
So change your data only if it still belongs to the failed operation:

- Pass a version of the operation in the payload, such as `generation` above, and check both the version and the expected state, such as `status = 'charging'`, in the update.
- Make the change, and enqueue the jobs that follow from it, in one transaction.
- Let an error propagate, such as of a database that is down: the failure job then retries, while catching it would lose the update.
- Send a notification or call another service with an idempotency key, such as `failure.id`.

A [retry](./failed-jobs#retry-a-failed-job) of the failed job keeps its payload, also its version, so to start the operation again, increment its version and enqueue a new job with it in one transaction, rather than retry the failed job.

## Add and remove `onFailure`

`onFailure` applies to the jobs that its workers take after its deploy, also to those enqueued before it; jobs that have failed already add no failure job.

Failure jobs left after you remove `onFailure` stay pending, as the jobs of a [removed task](../deployment/changing-tasks).
So keep the `failure` of the implementation in the manifest until the jobs that the old release took have ended and no failure jobs remain:

```ts [src/worker/manifest.ts]
// `handler` and `onFailure` are those that the implementation had.
const { failure } = chargeOrderTask.implement(handler, { onFailure })

export const manifest = defineManifest({
  tasks: [chargeOrderTask.implement(handler), failure],
})
```

## Test `onFailure`

Run the `failure` of the implementation with a `JobFailure` by [`runJob`](../testing#run-a-handler):

```ts
await runJob(chargeOrder.failure, {
  id: "1",
  reason: "handler-error",
  error: { message: "card declined" },
  failedAt: "2030-01-01T00:00:00.000Z",
  job: {
    id: "1",
    task: "orders.charge",
    scope: null,
    attempt: 25,
    maxAttempts: 25,
    key: null,
    queue: null,
  },
  payload: { orderId: "42", generation: 3 },
})
```
