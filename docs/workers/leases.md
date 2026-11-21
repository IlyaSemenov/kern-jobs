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

## Return the jobs of a restarted worker at once

To return the jobs of a worker that died as soon as it restarts, rather than after their leases, give the worker the same `id` at each start, such as the name of its process:

```ts
const worker = startWorker(driver, { manifest, id: "worker-1" })
```

A worker that starts with the id of a worker that has died returns the jobs of that worker before it takes any: each of them runs again on a worker of its task, or fails if that was its last attempt.
This helps a dev server that restarts on every change, and a process manager that restarts a crashed worker.
Without `id`, each worker gets an id of its own process, and the jobs of a dead worker wait for their leases.

Give each running worker its own id.
A worker whose id another running worker of the same [installation](../getting-started#create-the-schema) has takes no jobs, and [reports](../operations/monitoring#report-errors) a `heartbeat` error until that worker stops.

A worker that loses the connection that it keeps gives up its id until it reconnects.
A worker that starts with that id in the meantime returns the jobs of the first one, which may run again before their handlers end, and the first worker [aborts](./handlers#stop-long-handlers) them once it cannot extend their leases.

The worker holds its id on a connection that no other client may share; see [Connections of a worker](../database-clients#connections-of-a-worker).
