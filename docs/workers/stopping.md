# Stopping

Stop the worker before the process exits: `stopOnSignals` stops it at SIGTERM or SIGINT, then closes your database client and exits the process.

```ts [src/worker/start.ts]
import { stopOnSignals } from "kern-jobs/worker"

stopOnSignals(worker, { close: () => sql.end() })
```

The worker takes no more jobs and lets the running handlers end for 5 seconds; pass `abortAfter` in seconds to change it.
Then it [aborts their signals](./handlers#stop-long-handlers) and waits until they end and the worker has recorded their outcomes or given that up, such as without a database connection.
A job whose handler throws after the abort returns to pending at once without spending an attempt.
A job whose outcome is not recorded returns to pending once its lease expires, or fails if that was its last attempt.

The process exits within 25 seconds of the signal, also when a handler ignores its abort or a query hangs, since the worker cannot cancel them.
Pass `timeout` in seconds shorter than the time that your process manager gives the process before it kills it:

```ts
stopOnSignals(worker, { abortAfter: 3, timeout: 8, close: () => sql.end() })
```

A job whose handler still runs at the exit runs again once its lease expires, spending its attempt, or fails if that was its last attempt.
The same signal again, such as a second Ctrl+C, ends the process at once.

## Stop from your code

To stop a worker that runs beside your application, such as in the shutdown of your server, call `stop`, and close the database client after it:

```ts
await worker.stop({ abortAfter: 10, timeout: 20 })
await sql.end({ timeout: 5 })
```

`stop` stops the worker as above and resolves once it has stopped.
At `timeout`, it rejects instead: the worker starts no more database work, and it leaves a handler that still runs to its lease, which no new heartbeat extends, though one already sent may extend it once more.
It does not cancel the queries in flight, so exit the process rather than wait for them.
Without `timeout`, `stop` waits as long as the handlers run.
