# Graceful Shutdown

Stop the worker before the process exits: `stopOnSignals` stops it at SIGTERM or SIGINT, then calls `close`, such as to close your database client, and exits the process.

```ts [src/worker/start.ts]
import { stopOnSignals } from "kern-jobs/worker"

stopOnSignals(worker, { close: () => sql.end() })
```

The worker takes no more jobs and lets the running handlers end for 5 seconds; pass `abortAfter` in seconds to change it.
Then it [aborts their signals](./handlers#stop-long-handlers) and waits until they end: a job whose handler throws on the abort returns to pending at once without spending an attempt, unless the stop has been forced by then.
A job whose outcome the worker cannot record, such as without a database connection, returns to pending once its lease expires, or fails if that was its last attempt.

The process exits within 25 seconds of the signal, also when a handler ignores its abort, a query hangs, or `close` has not ended, since the worker cannot cancel them.
Pass `timeout` in seconds shorter than the time that your process manager gives the process before it kills it:

```ts
stopOnSignals(worker, { abortAfter: 3, timeout: 8, close: () => sql.end() })
```

A job whose handler still runs at the exit runs again once its lease expires, spending its attempt, or fails if that was its last attempt.

Another signal within 2 seconds of the first joins the stop, since one Ctrl+C can reach the process twice, such as when a script runner forwards it.
A later signal while the worker is still stopping [forces](#stop-from-your-code) the stop: the process calls `close` and exits with 1.
`stopOnSignals` stops listening once a signal has forced the stop, or once the stop has ended and 2 seconds have passed since the first signal; after that, SIGTERM and SIGINT have their usual effect again, such as ending the process at once, also during `close`, unless your application listens for them.

## Stop before your own shutdown

To run the shutdown of your application after the worker stops, such as to release its resources and send its error reports, use `waitForSignalAndStop`.
It stops the worker at SIGTERM or SIGINT as `stopOnSignals` does, and returns control to you instead of calling `close` and exiting:

```ts [src/worker/start.ts]
import { waitForSignalAndStop } from "kern-jobs/worker"

let code = 0
try {
  await waitForSignalAndStop(worker, { abortAfter: 5, timeout: 20 })
} catch (error) {
  console.error(error)
  code = 1
}
await sql.end({ timeout: 5 })
process.exit(code)
```

It resolves once the worker has stopped and rejects as [`stop`](#stop-from-your-code) does, such as at `timeout`.
Pass `timeout` so that the stop and then your shutdown end before your process manager kills the process; without it, the stop waits as long as the handlers run.
Exit the process yourself after your shutdown, since a handler that ignores its abort or a query in flight can keep it running.

Another signal works as with `stopOnSignals`: within 2 seconds of the first, it joins the stop, and later, while the worker is still stopping, it forces the stop, which rejects at once, so that your shutdown still runs.
It stops listening as `stopOnSignals` does, after which a signal can end the process during your shutdown.

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
