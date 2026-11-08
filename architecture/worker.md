# Worker

A worker runs the jobs of the tasks it implements: it claims them into free slots, runs their handlers under leases that its heartbeat keeps, settles them, and runs the reaper.

Claiming, heartbeats, and the reaper run in rounds, each one at a time: the next round starts an interval after the previous one ends, so a slow round delays the next one instead of overlapping it.

`startWorker` of `worker/worker.ts` checks the options of the application, fills their defaults, and starts the runtime of `worker/runtime.ts`, which trusts its options; tests run the runtime directly, with tasks, timings, and events that the application cannot give.

## Options

`startWorker` takes a driver of kern-db as its first argument, rather than the `Database` of the application, since the worker runs outside the transactions of the application ([decision](../decisions/drivers/worker-takes-driver.md)), and a manifest and these options as its second:

- `schemaName`: the PostgreSQL schema of the job table, `kern_jobs` by default, as of `createJobs` and `migrateJobs`.
- `concurrency`: the [slots](#slots) of the worker, 1 by default, an integer from 1 to 2^31 − 1, the largest limit of a statement.
- `id`: the name of the worker in the rows it claims, by default of its host, its process id, and a random suffix; a string that is not empty and has no U+0000, which PostgreSQL rejects in text.
- `lease`: the lease in seconds of tasks without one, 60 by default, in the [range of a lease](producer.md#task-definitions).
- `pollInterval`: seconds between claim cycles while slots are free, 1 by default.
- `reaper`: `false`, or `interval`, 5 seconds by default, and `limit`, 100 jobs per batch by default, of the [reaper](#reaper).
- `onError`: the receiver of [errors](#errors).

It throws for a wrong option before it starts anything: `TypeError` for a value that is not a driver of kern-db, such as a client passed instead of its driver, for a manifest, a `schemaName` that [`checkSchemaName`](producer.md#schema) rejects, an id, a `reaper`, or an `onError` of a wrong type, and `RangeError` for a number outside its range.
An option given as `undefined` takes its default, so that a wrapper can pass its optional parameters through.

An interval of a loop is above 0 and at most the longest delay of a timer, 2^31 − 1 milliseconds, in whole seconds, since a timer of a longer delay fires at once and the loop would run without a pause.

The heartbeat interval and the abort margin are not options: they are a third and a sixth of the shortest lease of the worker and its tasks, and the interval is at most the longest interval of a loop, since heartbeats more often than a lease needs are safe.
The worker reserves the heartbeat connection from the pool of its driver ([Drivers](drivers.md#connections-of-the-worker)).

## Tasks

A worker runs the jobs of the tasks of its manifest, which `toWorkerTasks` turns into the tasks of the runtime:

- `task.implement(handler)` pairs a task with the handler of its jobs, which gets the payload of the job and a context: the signal of [Leases](#leases) and the metadata of the job, its id, its scope (`null` for the empty scope), the number of the attempt from 1, `maxAttempts`, its key, and its queue.
- `defineManifest({ tasks })` takes implementations, one per task name: it throws `Error` for a name of several implementations, also for one implementation listed twice, so that a test that imports the manifest finds them, and `TypeError` for tasks that are not an array or an element that is not an implementation; it keeps a frozen copy of the list.
- `findUnimplementedTasks(tasks, manifests)` returns the names of the tasks that none of the manifests implements, for a test of the application, since no worker can tell a task without an implementation from a task of another worker; it takes the tasks as the values of an object, such as the exports of a module, skips the values without a string `name` and a function `implement`, and names each task once.
- `toWorkerTasks` checks the manifest and the lease, backoff, and `scoped` of each task again, as `defineManifest` and `defineTask` do, since any object of their shape passes the types, and gives each task the lease and the backoff of its definition.
- A job of a scoped task without a scope, such as one enqueued before the task became scoped, does not run its handler: the worker fails it with a `TerminalError`, so it becomes failed without retries, since its handler gets `job.scope` typed as a string.
- A handler fails its job for good with a `TerminalError`, and retries it after a given delay with a `RetryError`, such as the one of a `Retry-After` header, so that the error is decided where the handler meets it, and the definition, which producers import, holds no error classes of handlers ([decision](../decisions/tasks/handler-errors.md)).
- A backoff is a function of the number of the failed attempt that returns seconds; `exponentialBackoff` and `fixedBackoff` build common ones, and `defaultBackoff`, which `exponentialBackoff()` builds, waits 2 seconds after the first attempt, doubled after each next one, up to an hour.
- A delay of a retry is in the same [range](producer.md#options) as a delay of an enqueue; the factories and `RetryError` throw `RangeError` for a value outside it.
- The jitter of `exponentialBackoff` only shortens a delay, so that it never exceeds `max` and still spreads the retries that reached it.

## Schema check

Before its first claim, the worker checks that its schema has applied the migrations of its version of kern-jobs ([Migrations](migrations.md#check-of-a-worker)), since it does not migrate ([decision](../decisions/migrations/explicit-call.md)).

- Until the check passes, the worker neither claims nor starts its reaper: it reports the failure and checks again after the poll interval, also when the check itself fails, such as without a connection.
- The heartbeat starts with the worker, before the check: without claims, it needs no table.
- Once the check passes, the worker does not check again.
- The runtime takes the check as its option `checkSchema` and trusts the schema without one, as the tests of the runtime do.

## Readiness

`ready` of a worker resolves once it has checked its schema and a claim cycle has returned without an error.
The cycle checks that the heartbeat is healthy and then sends at least one claim statement on the pool, which proves the database, the schema, and a connection of the pool beside the one that the heartbeat holds.

- `ready` rejects when `stop` begins before it resolves; the worker handles that rejection itself, so that an application that never awaits `ready` gets no unhandled rejection.
- `state` is `starting` until then, `stopping` from the call of `stop`, and `stopped` once `stop` resolves or rejects.
- Between them, `state` is `ready` while the heartbeat is healthy, as [Claiming](#claiming) defines it, and `unhealthy` otherwise, so it follows the loss and the recovery of the database without an event; once ready, it does not return to `starting`.
- A claim or settlement that waits for a connection of a busy pool does not change `state`.

## Slots

- A worker has `concurrency` slots, and a job takes a slot from its claim until its settlement ends, not until its handler returns, so a slow settlement, such as a fail that waits for a producer, does not let the worker run more jobs than its slots.
- A handler that ignores its abort keeps its slot until it returns.
- A freed slot starts a claim cycle at once; while slots are free, the worker also claims every poll interval.

## Claiming

- The worker claims only while its heartbeat is healthy: the latest heartbeat succeeded, and none has been in flight for longer than the heartbeat interval.
  It checks this again before each claim statement of a cycle and ends the cycle when the heartbeat fails or the worker stops.
- A claim cycle fills the free slots, and each claim starts its handler as soon as its statement returns ([Claiming](claiming.md#claim-cycle)), unless:
  - the timeout of `stop` has passed: the worker sends nothing, and the job returns after its lease;
  - `stop` has begun: the worker requeues the job with its attempt given back, also when the deadline of its lease has passed too;
  - the deadline of its lease, the abort margin before its end, passed before its statement returned: the worker fails the job as by an error of its handler, `lease confirmation deadline passed before the handler started`, with the backoff of its task, and reports it, since a handler would start aborted, and the attempt counts, so that a claim statement that is always this slow cannot return the job forever.
- A failed claim cycle is reported, and the next one runs after the poll interval.
- A claim takes the lease of its task, or the lease of the worker for a task without one.

## Leases

The worker keeps the requirements of [Leases](leases.md#heartbeat):

- The heartbeat runs on a connection that the worker reserves for its life ([Drivers](drivers.md#connections-of-the-worker)).
- It sends one heartbeat at a time, every heartbeat interval, with every claim of the worker that is not known lost, and `select 1` without one, so that a broken connection is noticed before the worker claims.
- After a connection error, it releases the connection, which the driver does not return to the pool while it may be broken, and reserves another one for the next heartbeat; a statement that PostgreSQL rejected keeps the connection.
- The lease of a claim is confirmed until the send time of the latest statement that confirmed it, its claim statement or a heartbeat, plus the lease, on a monotonic clock, since the server extends the lease after the statement was sent; each claim statement of a cycle counts from its own send time.
- The heartbeat interval plus the abort margin must be shorter than the shortest lease, or a handler would be aborted between two heartbeats.
- The worker reaches a deadline beyond the longest delay of a timer, 2^31 − 1 milliseconds, by timers in steps, since a longer timer fires at once.

A handler gets an `AbortSignal`, which the worker aborts with a `HandlerAbort`, exported from `kern-jobs/worker`, and its reason:

- `lost`: a heartbeat found the claim lost;
- `deadline`: the lease is not confirmed the abort margin before it ends;
- `stop`: the worker stops, and the handler did not end within `abortAfter`, or the timeout of `stop` passed first.

After the handler ends, the worker aborts nothing, and the heartbeat keeps the lease until the settlement ends or the timeout of `stop` passes.

## Settlement

- A handler that returns completes its job, and one that throws fails it: a `TerminalError` for good, a `RetryError` with its delay, and any other error with the backoff of the task for the number of the attempt; the attempt counts in each case, so the last one fails for good, except after an interruption by `stop`.
- A handler whose signal was aborted with `stop` and that throws any other error than a `TerminalError` or a `RetryError` is interrupted: the worker [requeues](settlement.md#requeue) its job with the attempt given back and does not report the error, since libraries wrap an abort in errors of their own, and counting such an error as a failure would fail jobs for good at a deploy ([decision](../decisions/settlement/requeue-on-stop.md)); an abort `lost` or `deadline` does not change the outcome.
- A backoff that throws or returns a value outside the range of a delay is reported as `backoff-error`, and the job retries with `defaultBackoff`, so that it is still settled; so is a thrown value whose class `instanceof` cannot tell, such as a proxy whose trap throws.
- A claim that a heartbeat found lost is not settled.
- After a connection error, the worker retries the settlement on a new connection, with a delay that doubles up to the heartbeat interval, while the lease is confirmed; a statement that PostgreSQL rejected is retried three times ([decision](../decisions/settlement/worker-retries-settlement.md)).
- After a statement of unknown outcome, which `isOutcomeUnknown` of kern-db tells, a retry that finds the claim gone, or giving the settlement up, gives the outcome `unknown`: that statement may have been applied.
- A settlement given up without such a statement applied nothing, and the job stays active until the reaper returns it.

## Reaper

Every worker runs a reaper on its pool unless it is turned off: a batch every reaper interval, and the next batch at once after a full one.
A failed round is reported, and the next one runs after the interval, so the reaper resumes once the server is reachable again.
A producer that holds the key of an expired job does not stop the return of other expired jobs: the round waits for it a bounded time and returns them in the same round ([Leases](leases.md#reaper)).

## Stopping

`stop` takes `abortAfter`, 5 seconds by default, and `timeout`, none by default, both in seconds from its call, from 0 up to the longest interval of a loop; `startWorker` checks them on the first call, and a later call returns the promise of the first.
It runs these phases:

1. At once, the worker starts no claim cycle, and the cycle in flight sends no further claim statement; the heartbeat keeps the leases of the running jobs, and handlers that end are settled as usual.
2. At `abortAfter`, or at `timeout` if it comes first, the worker aborts the signals of the handlers that still run with `stop`.
3. Once the claim cycle and the check of the schema in flight have returned and every handler and its settlement have ended, it stops the reaper and the heartbeat, releases the heartbeat connection, and resolves.

The claim cycle in flight is awaited apart from the abort, since it may wait for a connection of the pool that a handler holds until its abort.

At `timeout`, if `stop` has not resolved, the worker gives up, then aborts the handlers that still run, so that one that ends on the abort is not settled, and rejects ([decision](../decisions/settlement/stop-timeout-leaves-claims.md)):

- It starts no operation any more: no heartbeat, no settlement or retry of one, no settlement of a handler that ends later, no requeue of a claim that returns later, and no round of the reaper.
- It cancels no operation that has started: a statement in flight, a settlement transaction with its retries after `23505`, and a round of the reaper end in the background, and the heartbeat releases its connection after its statement in flight.
- A connection that `acquire` of the heartbeat returns after the stop is released without a statement.
- A heartbeat sent before the timeout may still extend the leases of its claims once; nothing extends them after it.
- A handler that still runs keeps running: the worker neither settles nor requeues its job, which returns after its lease, with its attempt spent, as after a crash.

`stopOnSignals` of `worker/shutdown.ts` stops a worker at the first SIGTERM or SIGINT of the process, calls `close` of the application, and exits with 0, or with 1 after a failed stop or close, or once its `timeout`, 25 seconds by default, passes from the signal, since the worker cancels no query and cannot end a handler that ignores its abort.
The other of the two signals joins the stop, and the same signal again ends the process by its default action, as `process.once` leaves it.

## Errors

`startWorker` reports every error of the runtime synchronously to `onError`, with what failed, and ignores an error that `onError` throws:

- `handler`: the handler of a job threw, unless it was interrupted by `stop`; with the task, the metadata of the job, and the reason of the abort of its signal, or `null`.
- `backoff` and `settlement`: the backoff of a task failed, or a settlement statement did; with the task and the metadata of the job.
- `schema`: the schema is not up to date, or its check failed.
- `claim`, `heartbeat`, `reaper`, and `release`: a claim cycle, a heartbeat, a reaper round, or the release of a connection failed; `claim` also reports a claim that returned after the deadline of its lease, whose job the worker failed without running its handler.
- `worker`: an unexpected failure of the runtime, such as a defect.

Each failed statement is reported, also one that the worker retries.
Without `onError`, the worker writes the errors with `console.error`, so that a worker that cannot reach its database or its schema is not silent.

## Events

The runtime reports its events synchronously to `onEvent`, for tests and for `onError`, and ignores an error that `onEvent` throws.
The events are not public: an application observes the worker through `onError`, `ready`, and `state` ([Open questions](#open-questions)).

## Accepted limitations

- A handler that ignores the abort keeps its slot and delays `stop` until it returns or the timeout of `stop` passes; after its claim is lost or the timeout passes, it can overlap the next execution of its job ([Leases](leases.md#accepted-limitations)).
- Every worker reserves one connection of its pool for its heartbeats, so a pool of N connections stops N workers, and a pool of one connection stops its only worker: the check of the schema or the claims wait for a connection without an error, and the worker stays `starting`.
  Its `stop` then waits until its timeout, since it awaits the round in flight before it releases the heartbeat connection.
  The driver does not tell the size of its pool, so `startWorker` cannot check it.
- A lease has no minimum, since the pauses that it must outlast depend on the application.
  With S the shortest of the lease of a worker and the leases of its tasks, the heartbeat interval is S/3 and the abort margin S/6, so a job of lease L keeps its handler only while the previous heartbeat, the interval, a pause of the event loop, and the next heartbeat take less than L − S/6, and its claim statement must return within L − S/6, or the worker fails the job without running the handler.
  A pause plus two round trips longer than S/2 can therefore abort handlers with `deadline`, and a lease of milliseconds makes the worker heartbeat without a pause.
  The documentation asks for a shortest lease more than twice the sum of the longest pause and two round trips.
- Without `timeout`, `stop` waits for the handlers and the statements in flight, also for ones that never end.
- A claim statement in flight at the timeout of `stop` may still commit its claims; their jobs spend an attempt and return after their leases.
- An ordinary error that a handler throws after the abort `stop` counts as an interruption, also when it is a real failure: it is not reported, and the job runs again without spending its attempt, on every stop that interrupts it, beyond `maxAttempts`.
- After a claim statement whose connection broke, the server may have committed the claim; the job spends an attempt and returns after its lease.

## Open questions

- Whether the heartbeat interval and the abort margin become options.
- Public events beside `onError`, such as of completed jobs, which the table cannot count since complete deletes the job.
- Throughput with a pool smaller than the concurrency, when settlements wait for connections, is not measured.
- A run of several hours under load is not done.
