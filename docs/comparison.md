# Comparison

This page helps you choose between kern-jobs and two other job queues that keep their jobs in PostgreSQL and run on Node.js: [Graphile Worker](https://worker.graphile.org/) and [pg-boss](https://github.com/timgit/pg-boss).
The comparison uses the documentation and source code of Graphile Worker 0.18.0 and pg-boss 12.38.1, reviewed in November 2026.

## When kern-jobs fits

Choose kern-jobs when the jobs of your application follow its data:

- You enqueue jobs in the transactions of your application, through your own client, ORM, or kern-orm.
- Many events of one entity, such as edits of a document, should result in one pending job, debounced, but not postponed by the events beyond a bound.
- The jobs of one entity, such as syncs of an account, should run one at a time.
- The application and its workers should share the payload type of each task.

Consider another queue when you need what kern-jobs does not have yet:

- Waking workers by LISTEN/NOTIFY as soon as a job is enqueued, as [Graphile Worker](https://github.com/graphile/worker/blob/v0.18.0/website/docs/faq.md?plain=1#L22-L33) and [pg-boss](https://github.com/timgit/pg-boss/blob/12.38.1/docs/api/workers.md#low-latency-dispatch-with-listen-notify) can: a worker of kern-jobs polls for due jobs, every second unless you pass a shorter [`pollInterval`](./workers/#run-several-jobs-at-once).
- A queue that a failed job blocks until it is retried, as the policy `key_strict_fifo` of pg-boss does; a queue of kern-jobs moves on to its next job.
- Running recurring jobs in a local time zone or on an RRULE calendar with [pg-boss](https://github.com/timgit/pg-boss/blob/12.38.1/docs/api/scheduling.md#rrule-expressions).
- Priorities, rate limits, dependencies between jobs, or a dashboard.

kern-jobs is in early development, and its API changes between minor versions.

## At a glance

|                                                                          | kern-jobs                                                                 | Graphile Worker                                                               | pg-boss                                                        |
| ------------------------------------------------------------------------ | ------------------------------------------------------------------------- | ----------------------------------------------------------------------------- | -------------------------------------------------------------- |
| [Enqueue in a transaction of the application](#enqueue-in-a-transaction) | Through your client or ORM                                                | Through a SQL function                                                        | Through an adapter of your client                              |
| [Payload types shared by the application and workers](#payload-types)    | A task definition                                                         | A global interface                                                            | Not connected                                                  |
| [One pending job per entity](#one-pending-job-per-entity)                | Keys                                                                      | Job keys                                                                      | Queue policies, with other rules                               |
| [Debounce with a bound on postponing](#debounce)                         | Built in                                                                  | Without a bound                                                               | Time slots of a fixed length                                   |
| [Jobs of an entity one at a time](#jobs-of-an-entity-one-at-a-time)      | Queues                                                                    | Named queues                                                                  | `key_strict_fifo`, which also blocks on a failed job           |
| [Recurring jobs](#recurring-jobs)                                        | UTC calendar, one job within a recovery window                            | UTC crontab, backfill of each eligible occurrence                             | Cron or RRULE in a time zone, one catch-up job                 |
| [Jobs of a worker that dies](#jobs-of-a-worker-that-dies)                | After a lease, 60 s by default, or when a worker of its id starts         | Locked for at least 4 hours                                                   | Released after a heartbeat timeout or their expiration         |
| [Stopping a worker](#stopping-a-worker)                                  | Can return an interrupted job without spending its attempt                | A job that throws after the abort spends its attempt                          | Jobs still running at the timeout fail                         |
| [Work after a job fails for good](#work-after-a-job-fails-for-good)      | A handler in a job of its own, added with the failure, also by the reaper | A local event before the failure is recorded, none for a job of a dead worker | A dead letter copy, added with the failure, also at expiration |
| [Schema migrations](#schema-migrations)                                  | A call of the application                                                 | At the start of a worker                                                      | At the start of an instance                                    |

## Enqueue in a transaction

With kern-jobs, pass `createJobs` your client, the client of a transaction, or a function that returns the client of the current transaction; see [Database Clients and Transactions](./database-clients).

- Graphile Worker: call the SQL function [`graphile_worker.add_job`](https://github.com/graphile/worker/blob/v0.18.0/website/docs/sql-add-job.md) on the connection of the transaction.
- pg-boss: pass `send` a [`db` object](https://github.com/timgit/pg-boss/blob/12.38.1/docs/api/jobs.md?plain=1#L76-L86) with `executeSql`, which runs the statement on the connection of the transaction.

## Payload types

With kern-jobs, the definition of a task, which the application imports without the handler, types both the payload of an enqueue and that of the handler; see [Defining Tasks](./tasks/).

- Graphile Worker: declare the payload types of the tasks in the global interface [`GraphileWorker.Tasks`](https://github.com/graphile/worker/blob/v0.18.0/website/docs/typescript.md#assuming-type-via-graphileworkertasks).
- pg-boss: the type of the data that a handler of `work` receives is [not connected](https://github.com/timgit/pg-boss/blob/12.38.1/src/manager.ts#L1331-L1332) to the data passed to `send`.

## One pending job per entity

With kern-jobs, a key keeps at most one pending job, also one that waits for a retry, and an event while the job of the key runs adds the next pending job, which can start while the first one still runs; see [Deduplication and Debouncing](./tasks/keys).
To run the jobs of a key one at a time, also give them a [queue](#jobs-of-an-entity-one-at-a-time).

- Graphile Worker: a [job key](https://github.com/graphile/worker/blob/v0.18.0/website/docs/job-key.md#job_key_mode) keeps at most one pending job, including one waiting for a retry, and an event while its job runs adds a new job; but a key is unique across tasks, and the event sets the attempts of the running job to its maximum, so it does not run again after a failure.
- pg-boss: its [queue policies](https://github.com/timgit/pg-boss/blob/12.38.1/docs/api/queues.md?plain=1#L36-L43) differ: `short` and `stately` add a second job beside one that waits for a retry, and `exclusive` drops an event while the job of the key runs or waits for a retry.

## Debounce

With kern-jobs, for events with the same `delay` and `maxWait`, `keyMode: "debounce"` makes the pending job due `delay` seconds after the latest event, but no later than `maxWait` seconds after the first one; see [Debounce](./tasks/keys#debounce).
The bound limits how far the events postpone the job; busy workers or the backoff of a retry can still delay its start.

- Graphile Worker: a job key in the mode `replace` [postpones the job](https://github.com/graphile/worker/blob/v0.18.0/website/docs/job-key.md#job_key_mode) by each event without a bound, and `preserve_run_at` keeps the run time of the first event.
- pg-boss: [`sendDebounced`](https://github.com/timgit/pg-boss/blob/12.38.1/docs/api/jobs.md#senddebouncedname-data-options-seconds-key) accepts at most one job of a key in each time slot of a fixed length, and drops an event when the next slot has a job too.

## Jobs of an entity one at a time

With kern-jobs, a [queue](./tasks/queues) runs one of its jobs at a time.

- Graphile Worker: a [named queue](https://github.com/graphile/worker/blob/v0.18.0/website/docs/library/add-job.md) runs its jobs one at a time.
- pg-boss: the queue policy [`key_strict_fifo`](https://github.com/timgit/pg-boss/blob/12.38.1/docs/api/queues.md?plain=1#L43) runs the jobs of a key one at a time, and also holds them back while one of them waits for a retry or has failed; this comparison has not evaluated its other policies for this scenario.

## Recurring jobs

kern-jobs [schedules](./workers/schedules) use UTC calendars and can enqueue one job for all occurrences missed within a configured window.

- Graphile Worker: a [crontab](https://github.com/graphile/worker/blob/v0.18.0/website/docs/cron.md#distributed-crontab) in UTC, which several workers share, with a backfill of missed moments.
- pg-boss: cron or RRULE schedules in a chosen time zone can recover [one missed job alongside the current one](https://github.com/timgit/pg-boss/blob/12.38.1/docs/api/scheduling.md#catch-up-after-an-outage).

## Jobs of a worker that dies

With kern-jobs, a worker extends the leases of its running jobs, and when it dies, each of its jobs returns to pending once its lease expires, and runs again, or fails if that was its last attempt; see [Leases](./workers/leases).
The lease is 60 seconds by default, and a worker that starts with the `id` of the dead worker returns its jobs at once.

- Graphile Worker: a job of a worker that died stays locked for [at least 4 hours](https://github.com/graphile/worker/blob/v0.18.0/website/docs/error-handling.md#instantaneous-exit), unless you pass the ids of crashed workers to [`force_unlock_workers`](https://github.com/graphile/worker/blob/v0.18.0/website/docs/admin-functions.md?plain=1#L91-L125).
- pg-boss: such a job waits for its expiration, 15 minutes by default, or, with a [heartbeat](https://github.com/timgit/pg-boss/blob/12.38.1/docs/api/queues.md#heartbeat-vs-expiration) that its queue or job enables, until its heartbeats stop for longer than their timeout.

## Stopping a worker

With kern-jobs, a stopping worker lets its handlers end for 5 seconds, then aborts their signals; see [Graceful Shutdown](./workers/stopping).
A job whose handler then throws, other than a `TerminalError` or a `RetryError`, returns to pending without spending its attempt once the worker records the outcome.

- Graphile Worker: it waits for its running jobs and [aborts their signal](https://github.com/graphile/worker/blob/v0.18.0/website/docs/error-handling.md#termination-signal) after [5 seconds](https://github.com/graphile/worker/blob/v0.18.0/website/docs/config.md#workergracefulshutdownaborttimeout); a job whose handler then throws spends its attempt.
- pg-boss: [`stop`](https://github.com/timgit/pg-boss/blob/12.38.1/docs/api/ops.md#stopoptions) waits for the running handlers for 30 seconds, then fails their jobs.

## Work after a job fails for good

With kern-jobs, [`onFailure`](./workers/failure-handlers) runs in a job that is added in the transaction that records the failure, also when the worker died on the last attempt, and that retries as any job.
A job that a stop returns to pending without spending its attempt adds none.

- Graphile Worker: a worker emits the local event [`job:failed`](https://github.com/graphile/worker/blob/v0.18.0/src/worker.ts#L295-L356) before it records the failure, and a job whose worker died on its last attempt [is no longer taken](https://github.com/graphile/worker/blob/v0.18.0/sql/000011.sql#L67) and gets no event.
- pg-boss: the statement that fails a job after its retries, [also at its expiration](https://github.com/timgit/pg-boss/blob/12.38.1/src/plans.ts#L3268-L3296), inserts a copy of its payload into the [`deadLetter`](https://github.com/timgit/pg-boss/blob/12.38.1/docs/api/queues.md?plain=1#L69-L71) queue of its queue, also of a job that its stop failed at the timeout.

## Schema migrations

With kern-jobs, the application calls `migrateJobs`, such as at every deploy, and a worker only checks that the schema is up to date; see [Create the schema](./getting-started#create-the-schema).

- Graphile Worker: its runner [migrates the schema](https://github.com/graphile/worker/blob/v0.18.0/src/lib.ts#L509-L512) at its start.
- pg-boss: [`start`](https://github.com/timgit/pg-boss/blob/12.38.1/docs/api/constructor.md#migrate) migrates the schema, and with `migrate: false` rejects a schema that needs a migration.
