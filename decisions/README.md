# Decisions

Trade-offs between designs of kern-jobs: what each decision chose, the alternatives it weighed, their measured costs, and the other projects that chose an alternative.
Architecture states the chosen designs and links each decision from the contract of its design.

Measurements come from one machine without network, so they compare designs rather than promise throughput.

## Enqueue

- [Keys unique per scope and task](enqueue/key-per-task.md).
- [A task is scoped, and the jobs API gives the scope](enqueue/scope-of-jobs-api.md): a string or a function of the current request.
- [`keep` locks the pending row](enqueue/keep-locks-pending-row.md): every `keep` writes a row version.
- [`replace` keeps attempts](enqueue/replace-keeps-attempts.md).
- [`maxWait` bounds the computed run time](enqueue/max-wait-bounds-run-at.md), not the claim.
- [A debounce window opens at its first debounce event, and its run time never moves backwards](enqueue/debounce-window.md).
- [`enqueueMany` takes the payloads of one task](enqueue/batch-of-one-task.md), rather than items of several tasks.
- [A key at most once per `enqueueMany`](enqueue/distinct-keys-in-batch.md), rather than the last item of a key winning.
- [`enqueueMany` inserts in the order of its items](enqueue/batch-insert-order.md): concurrent batches with keys in different orders can deadlock.
- [The SQL function copies the statement of the core](enqueue/sql-function-copies-the-statement.md), which a test compares with it, rather than the core calling the function.
- [The SQL function binds its table at its creation](enqueue/sql-function-binds-its-table.md): a SQL-standard body, with its checks in a second function.

## Schedules

- [A durable cursor for each schedule](schedules/occurrence-cursor.md).
- [One job per recovery window](schedules/one-job-per-recovery-window.md).
- [One definition per schedule ID](schedules/one-definition-per-id.md): a changed definition takes a new ID.

## Claiming

- [Queue mutex by a unique index](claiming/queue-mutex-index.md).
- [A busy queue head skips the queue](claiming/busy-queue-head.md).
- [Queue scan by a loose index scan](claiming/queue-scan.md).
- [Slots split between jobs with and without a queue](claiming/slot-split.md).

## Drivers

- [Typed table queries for simple worker mutations](drivers/worker-orm.md).
- [Hand-written worker statements are kern-orm templates](drivers/worker-templates.md), rather than SQL text with parameters cast from text.
- [Parameters and results as text](drivers/text-values.md).
- [A transaction owns its connection](drivers/transaction-connection.md): a worker needs a pool.
- [Enqueues through the client of the application](drivers/executor.md): its results are read by position, as text.
- [The worker runs on the drivers of kern-db](drivers/kern-db-drivers.md): kern-jobs exposes the drivers supplied by kern-orm through its own subpaths.
- [The worker takes a driver, not the database of the application](drivers/worker-takes-driver.md): its transactions stay out of those of the application.

## Migrations

- [kern-jobs applies its own migrations](migrations/own-history.md), on a driver of kern-db rather than on the client of the application.
- [The application calls `migrateJobs`, and a worker only checks](migrations/explicit-call.md): a worker does not migrate at its start.

## Settlement

- [Merge on failure by the key mode of the pending job](settlement/merge-on-failure.md).
- [The worker retries a settlement after connection loss](settlement/worker-retries-settlement.md): a job keeps its slot until then.
- [A job interrupted by stop returns without spending its attempt](settlement/requeue-on-stop.md): an error after the abort counts as an interruption unless it is a `TerminalError` or a `RetryError`.
- [When it gives up, stop leaves the jobs of running handlers to their leases](settlement/stop-timeout-leaves-claims.md), rather than failing or requeuing them at once.
- [A terminal failure inserts an ordinary job that runs `onFailure`](settlement/failure-jobs.md), stamped by the claim, rather than a callback in the worker, a table of deliveries, a state of the failed row, or a failure task that the application defines.

## Tasks

- [Static options in `defineTask`, options of a payload in `withOptions`](tasks/definition-shape.md).
- [The lease and the backoff are code of the task](tasks/worker-settings.md): the row stores neither.
- [Handlers decide terminal errors and retry delays by error classes](tasks/handler-errors.md).

## Leases

- [Fencing by a claim id](leases/claim-id-fencing.md).
- [Heartbeats on a connection of their own](leases/heartbeat-connection.md).
- [Heartbeats skip locked rows](leases/heartbeat-skips-locked-rows.md): a busy claim is neither extended nor lost.
- [Ownership by the claim, not the lease](leases/ownership-by-claim.md).
- [The reaper detaches instead of merging](leases/reaper-detaches.md).
- [The reaper bounds its wait and leaves the blocked row](leases/reaper-bounds-its-wait.md).
- [A worker returns the jobs of its id under a lock of the id](leases/worker-id-lock.md): a worker that starts with the id of an ended worker returns its jobs without waiting for their leases.

## Administration

- [A retry keeps both jobs when a pending job of its key exists](administration/retry-keeps-both-jobs.md), in every key mode.
- [A retry gives a full budget of attempts](administration/retry-resets-attempts.md), and no backoff.
- [A retry updates the row in one statement on the client of the application](administration/retry-updates-the-row.md): an enqueue that adds a new pending job of the key at the same time can make it fail with `23505`.
- [An application deletes jobs from SQL by a statement of its own](administration/delete-from-sql.md), with a condition on their state, rather than by SQL functions of the schema: the columns of its conditions bind later migrations.
- [The scope of the jobs API limits administration](administration/scope-limits-administration.md): without a scope, to unscoped jobs; `ALL_SCOPES` reaches every scope.
- [Metrics read a summary view](administration/summary-view.md), not the columns of the job table.

## Worker

- [A later signal forces the stop only once 2 seconds have passed since the first](worker/signal-window.md): a doubled Ctrl+C joins the stop, also during the cleanup of the application.

## Testing

- [The test double of the jobs API keeps its jobs in memory](testing/in-memory-double.md), not in the job table of the transaction of the test.

## Writing a decision

- Write each decision in a file of its own under its topic directory, such as `decisions/claiming/`, and list it in that topic's section of this index; add a topic only when none fits.
- Write in it the chosen design, the alternatives it weighs, their measured costs, and the other projects that chose an alternative.
- Mention another project only when its design is an alternative that the decision weighs, and name it as [comparison/README.md](../comparison/README.md#writing-a-record) describes.
- Add readable examples where they make the designs easier to compare, such as the SQL of each design.
- Record the ratios and counts that a decision relies on rather than absolute times.
- Link a decision from the contract of its design in architecture/, and keep there only that design and a reason of one clause; do not link decisions/ back to architecture/.
- In a comment beside the code of a decision, state its reason, with a measurement only when the reason rests on it.
