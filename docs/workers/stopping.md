# Graceful Shutdown

Stop the worker before the process exits: `stopOnSignals` stops it at SIGTERM or SIGINT, then closes your database client and exits the process.

```ts [src/worker/start.ts]
import { stopOnSignals } from "kern-jobs/worker"

stopOnSignals(worker, { close: () => sql.end() })
```

The worker takes no more jobs and lets the running handlers end for 5 seconds; pass `abortAfter` in seconds to change it.
Then it [aborts their signals](./handlers#stop-long-handlers) and waits until they end: a job whose handler throws on the abort returns to pending at once without spending an attempt.
A job whose outcome the worker cannot record, such as without a database connection, returns to pending once its lease expires, or fails if that was its last attempt.

The process exits within 25 seconds of the signal, also when a handler ignores its abort or a query hangs, since the worker cannot cancel them.
Pass `timeout` in seconds shorter than the time that your process manager gives the process before it kills it:

```ts
stopOnSignals(worker, { abortAfter: 3, timeout: 8, close: () => sql.end() })
```

A job whose handler still runs at the exit runs again once its lease expires, spending its attempt, or fails if that was its last attempt.
The same signal again, such as a second Ctrl+C, ends the process at once.

## Stop from your code

To stop a worker that runs beside your application, such as in the shutdown of your server, call `stop`, and close the database client after it, also when `stop` rejects:

```ts
try {
  await worker.stop({ abortAfter: 10, timeout: 20 })
} finally {
  await sql.end({ timeout: 5 })
}
```

`stop` stops the worker as above and resolves once the handlers have ended and the worker has finished its attempts to record their outcomes, so the database client can close then; without `timeout`, it waits as long as the handlers run.
At `timeout`, it stops waiting and rejects: the worker starts no more database work and records the outcome of no handler that ends later, and the job of such a handler returns once its lease expires with its attempt spent, or fails if that was its last attempt.
A database operation that has already begun is not cancelled and may end after the rejection, such as a check of a schedule that still enqueues its job, so exit the process rather than wait for it.

`force` aborts the signals of the running handlers at once, without waiting for `abortAfter`, and rejects as at `timeout`, also the promise of a call of `stop` that is already waiting, such as at a second Ctrl+C:

```ts
const stopped = worker.stop({ abortAfter: 10 })
// later, when the stop takes too long
worker.stop({ force: true })
await stopped // rejects
```

Another call without `force` returns the promise of the first and changes nothing.
