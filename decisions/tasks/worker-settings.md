# The lease and the backoff are code of the task

A task gives its `lease` and `backoff` to `defineTask`, and the worker reads them from the definition: the lease when it claims a job, and the backoff when a job fails.
The row stores neither, and an enqueue cannot override them.

A claim statement takes a batch of jobs of several tasks and sets the lease of each row from a map of task leases, which the worker passes, before it reads any payload.
A backoff is a function of the number of the attempt, so it covers any schedule, such as a table of delays, and factories build the common ones:

```ts
defineTask("payments.charge", {
  lease: 120,
  backoff: exponentialBackoff({ base: 10, jitter: 0.2 }),
})
```

## Alternative

The enqueue could store the lease and a declarative backoff in the row, such as `{ base, factor, max }`, as options of `withOptions` and of an enqueue.
A job could then take a lease and a backoff computed from its payload or given by its producer, keep them across a deploy that changes the task, and a tool that reads the table could show its retry schedule.
The cost is columns of the row, a claim that reads the lease from each row, and a policy of the worker that producers decide.

Other projects store the retry policy with each job:

- pg-boss takes the retry and expiration options of a job at `send`, with the defaults of its queue, and stores them in the row.
- BullMQ takes `attempts` and `backoff: { type, delay }` as options of a job, and functions of custom backoffs as settings of the worker.
