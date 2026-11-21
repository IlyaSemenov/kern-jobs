# Keys

## An event while a job of the same key runs or waits for a retry

A key keeps at most one pending job, also one that waits for a retry, and an event while the job of the key runs becomes the next pending job.

- Graphile Worker, same: an event while the job of its key runs adds a new job, and an event while it waits for a retry updates it ([docs](https://github.com/graphile/worker/blob/v0.18.0/website/docs/job-key.md#job_key_mode)).
- pg-boss, lacks: no queue policy keeps one job of a key that waits to run or for a retry while letting an event during a running job add the next one.
  `short` and `stately` add a second job beside the one that waits for a retry, so both run; `exclusive` resolves `send` with `null` while the job of the key runs or waits for a retry, so no job starts after an event during a run ([docs](https://github.com/timgit/pg-boss/blob/12.38.1/docs/api/queues.md?plain=1#L36-L43), [source](https://github.com/timgit/pg-boss/blob/12.38.1/src/plans.ts#L1363-L1371), [source](https://github.com/timgit/pg-boss/blob/12.38.1/src/plans.ts#L1407)).

## A failure of the running job after an event of its key

If the running job fails with a retryable error and has attempts left while an event of its key has added a pending job, the two jobs merge by the key mode of the pending job.
`replace` and `debounce` delete the failed job, and `keep` deletes the pending one.
A job that fails for good, such as by `TerminalError`, is recorded as failed without a merge.

- Graphile Worker, differs: the event sets the attempts of the running job to its maximum, so the job does not run again after a failure or after its worker dies, and the new job runs ([docs](https://github.com/graphile/worker/blob/v0.18.0/website/docs/job-key.md#job_key_mode)).

## Attempts of a replaced job

`replace` keeps the attempts of the pending job, also of one that waits for a retry.

- Graphile Worker, differs: `replace` resets the attempts to 0 ([docs](https://github.com/graphile/worker/blob/v0.18.0/website/docs/job-key.md#job_key_mode)).

## Keys and queues of tenants

A task defined with `scoped: true` takes the scope of each job from `createJobs`, such as the tenant of the current request, and its keys merge and its queues serialize only the jobs of their scope.

- graphile-worker-tasks 2.5.0, differs: a definition of a tenant task, which the application wraps, stores the tenant of the context in the payload and prefixes the key and the queue with it, and its handler runs in the context of the tenant ([docs](https://github.com/IlyaSemenov/graphile-worker-tasks/blob/v2.5.0/README.md#wrapping-tasks)).

## The same key in several tasks

Keys of different tasks are independent, and `replace` never changes the task of a job.

- Graphile Worker, differs: a key is unique across tasks, and an enqueue of another task with the same key replaces the task of the pending job ([docs](https://github.com/graphile/worker/blob/v0.18.0/website/docs/job-key.md?plain=1#L142-L147)).

## Debounce with a bound on the wait

For events with the same `delay` and `maxWait`, `debounce` makes the pending job due `delay` seconds after the latest event, but no later than `maxWait` seconds after the first one.
The bound limits only how far the events postpone the job: a backoff after a failure can still make it due later.

- Graphile Worker, lacks: `replace` postpones the job by each event without a bound, and `preserve_run_at` keeps the run time of the first event ([docs](https://github.com/graphile/worker/blob/v0.18.0/website/docs/job-key.md#job_key_mode)); debounce with a maximum delay was proposed in [graphile/worker#583](https://github.com/graphile/worker/issues/583).
- pg-boss, differs: `sendDebounced` accepts at most one job of a key in each time slot of a fixed length.
  When the current slot has a job, it schedules the event in the next slot, and drops it when that slot has a job too ([docs](https://github.com/timgit/pg-boss/blob/12.38.1/docs/api/jobs.md#senddebouncedname-data-options-seconds-key)).

## The same key twice in one call

`enqueueMany` rejects two payloads whose options give the same key before it runs its statement, since the enqueues of the payloads one after another would merge them by their modes, which one statement cannot.

- Graphile Worker, differs: `add_jobs` does not check the keys of its jobs, and its `INSERT … ON CONFLICT DO UPDATE` would update the row of a key twice, which PostgreSQL rejects, so the call fails ([source](https://github.com/graphile/worker/blob/v0.18.0/sql/000020.sql#L140-L190)).
- pg-boss, differs: `insert` skips a job whose `singletonKey` an earlier job of the call took, under the policies that keep one job of a key, so the first job wins, and rejects two jobs of the same explicit `id` before its statement ([source](https://github.com/timgit/pg-boss/blob/12.38.1/test/insertTest.ts#L238-L298), [source](https://github.com/timgit/pg-boss/blob/12.38.1/src/manager.ts#L2016-L2023)).
- BullMQ, differs: of jobs of one `jobId` without a deduplication id, the first wins and the later one resolves to its id: on Redis, the script of each job in turn finds the job of the id, and on PostgreSQL, the statement of `add_jobs_bulk`, which takes a call whose jobs have neither a parent nor a deduplication id, skips it by `ON CONFLICT DO NOTHING` ([source](https://github.com/taskforcesh/bullmq/blob/v6.7.5/src/commands/includes/handleDuplicatedJob.lua#L27-L30), [source](https://github.com/taskforcesh/bullmq/blob/v6.7.5/src/postgres/migrations/0002_functions.sql#L3428-L3471)).
  Jobs with a deduplication id take another path on PostgreSQL, `add_flow`, which deduplicates each job before its insert, and in `replace` mode a later job replaces an earlier one that still waits for its delay ([source](https://github.com/taskforcesh/bullmq/blob/v6.7.5/src/postgres/postgres-queue-backend.ts#L625-L643), [source](https://github.com/taskforcesh/bullmq/blob/v6.7.5/src/postgres/migrations/0008_bulk_dedup_lock_order.sql#L64-L78), [source](https://github.com/taskforcesh/bullmq/blob/v6.7.5/src/postgres/migrations/0006_dedup_reschedule.sql#L39-L65)).
- Oban, lacks: `insert_all` of the Basic engine ignores unique options, so two jobs with the same unique options are both inserted ([docs](https://github.com/oban-bg/oban/blob/v2.24.2/lib/oban.ex#L816-L821)).
