# Enqueue

## Many jobs in one call

`enqueueMany` enqueues the payloads of one task in one statement, all or none, and resolves to the id of the job of each payload, in their order, and whether it inserted the job rather than merged it into the pending job of its key.

- Graphile Worker, differs: `addJobs` takes jobs of several tasks and queues, and adds them by one call of the SQL function `add_jobs`, all or none ([docs](https://github.com/graphile/worker/blob/v0.18.0/website/docs/library/add-jobs.md#L42-L94), [source](https://github.com/graphile/worker/blob/v0.18.0/sql/000020.sql#L95-L190)).
  It pairs the rows that the statement returns with the jobs by position, and tells an inserted job from a replaced one only by its `revision` ([source](https://github.com/graphile/worker/blob/v0.18.0/src/helpers.ts#L83-L118)).
- pg-boss, differs: `insert` takes the jobs of one queue and adds them in one statement with `ON CONFLICT DO NOTHING`, which skips a job whose `id` a job already has, or whose `singletonKey` a job already holds under a policy or a throttle slot that keeps one job of a key, and inserts the others ([docs](https://github.com/timgit/pg-boss/blob/12.38.1/docs/api/jobs.md#L338-L371), [source](https://github.com/timgit/pg-boss/blob/12.38.1/src/plans.ts#L3113-L3204)).
  With `returnId`, it resolves to the ids of the inserted jobs, which are not guaranteed to line up with the input by position, or to `null` when it inserts none ([docs](https://github.com/timgit/pg-boss/blob/12.38.1/docs/api/jobs.md#L369-L371)).
- BullMQ, differs: `addBulk` takes jobs of one queue, each with its name; on Redis it runs a script per job in a pipeline, so a failure of one script can leave other jobs added, and on PostgreSQL it adds them in one statement ([source](https://github.com/taskforcesh/bullmq/blob/v6.7.5/src/classes/redis-queue-backend.ts#L704-L735), [docs](https://github.com/taskforcesh/bullmq/blob/v6.7.5/docs/gitbook/guide/queues/adding-bulks.md#L119-L121), [source](https://github.com/taskforcesh/bullmq/blob/v6.7.5/src/postgres/postgres-queue-backend.ts#L606-L643)).
  It resolves to the jobs in the order of the input, and a job of an existing id gets that id without a flag ([source](https://github.com/taskforcesh/bullmq/blob/v6.7.5/src/classes/job.ts#L292-L329)).
- Sidekiq, differs: `push_bulk` takes the arguments of one job class and pushes them in slices of `batch_size`, by default 1000 jobs, or 100 with a time to run at, without a transaction, so the slices before a failure stay pushed; it returns the job ids in the order of the input, with `nil` for a job that a middleware of the client stopped ([source](https://github.com/sidekiq/sidekiq/blob/v8.1.7/lib/sidekiq/client.rb#L113-L182)).
- Oban, differs: `insert_all` takes jobs of several workers, which the Basic engine inserts in one statement, ignoring their unique options ([docs](https://github.com/oban-bg/oban/blob/v2.24.2/lib/oban.ex#L792-L865), [source](https://github.com/oban-bg/oban/blob/v2.24.2/lib/oban/engines/basic.ex#L90-L96)).

## Enqueue from SQL

The SQL function `enqueue` of the schema enqueues a job with the options and the key modes of an enqueue, such as from a migration of the application, and returns its id and whether it inserted the job rather than merged it into the pending job of its key.

- Graphile Worker, same: the SQL function `graphile_worker.add_job` enqueues a job with its options, and its `addJob` calls that function too ([docs](https://github.com/graphile/worker/blob/v0.18.0/website/docs/sql-add-job.md)).
