# Administration

## A retry of a job that failed for good

A retry keeps the id of the job, gives it the full budget of its attempts, and runs it at once.

- Graphile Worker, differs: `rescheduleJobs` updates the job with the attempts, the maximum, and the run time that it is given, and leaves the others, so a job that failed for good runs again only with fewer attempts or a higher maximum, and without a run time it waits out the backoff of its last failure ([docs](https://github.com/graphile/worker/blob/v0.18.0/website/docs/admin-functions.md#L59-L89), [source](https://github.com/graphile/worker/blob/v0.18.0/sql/000016.sql#L250-L268)).
- pg-boss, differs: `retry` keeps the id and the count of retries and raises their limit by one, so a job that spent its retries runs once more, at once or at its own later start time ([source](https://github.com/timgit/pg-boss/blob/12.38.1/src/plans.ts#L4150-L4166)).
- BullMQ, differs: `job.retry` keeps the id and runs the job at once, and keeps its attempts unless it is told to reset them, so a job that spent its attempts runs once more ([docs](https://github.com/taskforcesh/bullmq/blob/v6.7.5/docs/gitbook/guide/jobs/retrying-job.md#L119-L241)).
- Sidekiq, differs: a retry of a dead job keeps its jid and pushes it at once with its retry count less one, so a job that spent its retries runs once more ([source](https://github.com/sidekiq/sidekiq/blob/v8.1.7/lib/sidekiq/api.rb#L642-L650)).

## A retry while a pending job of the key exists

The retry changes neither job and reports the pending job, so that the operator decides which of them runs.

- Graphile Worker, differs: an enqueue of the key after the failure takes the key from the failed job and adds a new job, so a rescheduled failed job runs beside it ([source](https://github.com/graphile/worker/blob/v0.18.0/sql/000020.sql#L120-L133)).
- pg-boss, differs: `retry` updates the failed job without handling a conflict, so, by its SQL, another job of the key makes it throw a unique violation under the policy `exclusive`, and both jobs run under `short` and `singleton` ([source](https://github.com/timgit/pg-boss/blob/12.38.1/src/plans.ts#L4150-L4166), [docs](https://github.com/timgit/pg-boss/blob/12.38.1/docs/api/queues.md#L36-L58)).
  Its `redrive` from a dead letter queue inserts a new job and leaves a colliding one failed with `redrive_conflict` ([docs](https://github.com/timgit/pg-boss/blob/12.38.1/docs/api/jobs.md#L571-L616)).
- BullMQ, differs: a job that fails releases its deduplication id, unless the id has a time to live, and a retry does not take it back, so a job of the same deduplication id added after the failure runs beside it ([source](https://github.com/taskforcesh/bullmq/blob/v6.7.5/src/commands/includes/removeDeduplicationKeyIfNeededOnFinalization.lua#L6-L23)).

## Deleting a running job

A delete leaves a running job as it is, and its handler runs on.

- Graphile Worker, differs: `completeJobs` and `permanentlyFailJobs` skip a locked job unless its lock is older than four hours, when they delete or fail it, and `removeJob` of a running job takes its key and spends its attempts, without stopping the handler ([source](https://github.com/graphile/worker/blob/v0.18.0/sql/000016.sql#L175-L217), [source](https://github.com/graphile/worker/blob/v0.18.0/sql/000018.sql#L177-L206)).
- pg-boss, differs: `deleteJob` deletes an active job, and `cancel` makes it `cancelled`; with heartbeats of its queue, the next heartbeat aborts the signal of the handler, and its completion changes nothing ([source](https://github.com/timgit/pg-boss/blob/12.38.1/src/plans.ts#L1811-L1825), [docs](https://github.com/timgit/pg-boss/blob/12.38.1/docs/api/workers.md#L345)).
- BullMQ, same: `remove` refuses a job that a worker holds ([docs](https://github.com/taskforcesh/bullmq/blob/v6.7.5/docs/gitbook/guide/jobs/removing-job.md#L95-L99)).
- Sidekiq, same: a running job has left its queue, and no call deletes it ([source](https://github.com/sidekiq/sidekiq/blob/v8.1.7/lib/sidekiq/fetch.rb#L48)).
