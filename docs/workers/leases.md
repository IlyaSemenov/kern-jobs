# Leases

While a handler runs, the worker extends the lease of its job.
When a worker dies, the leases of its jobs expire, and the running workers return these jobs to pending, so each of them runs again, or fails if that was its last attempt.

A lease lasts 60 seconds.
A shorter lease runs the jobs of a dead worker again sooner, and a longer one lets handlers survive a longer loss of the database connection.

Keep the shortest of the lease of a worker and the leases of its tasks more than twice the sum of the longest pause of its event loop, such as by a computation without `await`, and two round trips to the database.
The worker extends the leases every third of the shortest one and [aborts](./handlers#stop-long-handlers) a handler whose lease it could not extend a sixth of the shortest lease before the end, so a longer pause can abort the running handlers, and their jobs run again.

To change the lease of every task of a worker, pass `lease` in seconds to `startWorker`:

```ts
const worker = startWorker(driver, { manifest, lease: 30 })
```

To change the lease of one task, pass `lease` to `defineTask`, which the workers apply instead of their own:

```ts
export const syncAccountTask = defineTask("accounts.sync", { lease: 300 }).withPayload<{
  accountId: string
}>()
```

A handler can run longer than its lease, since the worker keeps extending it.
