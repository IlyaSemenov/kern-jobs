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
