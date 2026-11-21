# Queues

## One running job per queue

A queue runs at most one job at a time per scope, also when a worker claims several jobs at once, since a unique index of active jobs rejects a second one.

- Graphile Worker, same: it runs jobs of one named queue serially ([`queueName`](https://worker.graphile.org/docs/library/add-job)).

## The order of the new jobs of one call

`enqueueMany` inserts its new jobs in the order of the payloads, so their ids follow it, and a queue starts those of equal run time in that order.

- Graphile Worker, differs: a worker takes jobs by `priority` and `run_at` without the id, so the jobs of one `addJobs` of equal priority and run time start in no defined order ([source](https://github.com/graphile/worker/blob/v0.18.0/src/sql/getJobs.ts#L231-L241)).
- pg-boss, differs: the jobs of one `insert` without an explicit `id` get random ids and the same `created_on`, by which a worker takes them after `priority`, so those of equal priority start in no defined order ([source](https://github.com/timgit/pg-boss/blob/12.38.1/src/plans.ts#L3140-L3146), [source](https://github.com/timgit/pg-boss/blob/12.38.1/src/plans.ts#L2735-L2756)).
- BullMQ, same: on PostgreSQL, the jobs of one `addBulk` of equal priority get a sequence in the order of the input, by which a worker takes them first in, first out, unless they ask for `lifo` ([source](https://github.com/taskforcesh/bullmq/blob/v6.7.5/src/postgres/migrations/0002_functions.sql#L3404-L3421)).
- Sidekiq, differs: `push_bulk` pushes the jobs to run now in the order of the input, and a worker takes the first one first, but it adds the jobs to run at a time to a sorted set, which orders the jobs of equal time by their serialized form rather than by the input ([source](https://github.com/sidekiq/sidekiq/blob/v8.1.7/lib/sidekiq/client.rb#L284-L302), [source](https://github.com/sidekiq/sidekiq/blob/v8.1.7/lib/sidekiq/fetch.rb#L48), [source](https://github.com/sidekiq/sidekiq/blob/v8.1.7/lib/sidekiq/scheduled.rb#L12-L36), [docs](https://redis.io/docs/latest/commands/zrange/#common-behavior-and-options)).
- Oban, differs: a worker takes jobs by `priority`, `scheduled_at`, and id, but nothing documents that the ids of one `insert_all` follow the order of its jobs ([source](https://github.com/oban-bg/oban/blob/v2.24.2/lib/oban/engines/basic.ex#L115)).
