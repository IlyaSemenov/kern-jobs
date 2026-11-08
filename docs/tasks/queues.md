# Queues

To run jobs one at a time, such as those that update the same external account, give them a `queue`.
A queue runs one of its jobs at a time and can hold the jobs of several tasks of one [scope](./scopes).

```ts
export const syncAccountTask = defineTask("accounts.sync")
  .withPayload<{ accountId: string }>()
  .withOptions(({ accountId }) => ({ queue: `account:${accountId}` }))
```

The name of a queue is at most 1024 bytes long.

A queue starts the job that is due earliest, but does not keep a strict order:

- A job that runs again after a failure or after its worker dies waits behind the jobs that became due before it.
- A worker starts only the jobs of the tasks in its manifest, so when workers with different manifests serve one queue, a job of one task can start before an earlier job of another.
- An enqueue that adds a job or changes its run time while a worker takes the next job of the queue can start them out of order.

A handler that ignores its abort signal can still run beside the next job of its queue after its worker [loses its job](../workers/handlers#stop-long-handlers).
