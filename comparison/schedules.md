# Schedules

## Recovery of a recurring job

kern-jobs evaluates the current UTC minute, including at first registration, and can enqueue one job if any matching minute was missed within the configured window.
A current occurrence consumes that single enqueue.

- Graphile Worker differs: its regular loop rounds startup forward to the next minute, and bounded backfill queues each eligible missed occurrence of a known schedule ([source](https://github.com/graphile/worker/blob/v0.18.0/src/cron.ts#L365-L389), [backfill](https://github.com/graphile/worker/blob/v0.18.0/website/docs/cron.md#limiting-backfill)).
- pg-boss differs: it sends cron and RRULE occurrences due in the preceding 60 seconds.
  After downtime, the default `missed: 'skip'` skips earlier occurrences, and `missed: 'once'` can send one catch-up job alongside a currently due one ([scheduling](https://github.com/timgit/pg-boss/blob/12.38.1/docs/api/scheduling.md#catch-up-after-an-outage)).

## A recurring job for each tenant

A schedule of a scoped task enqueues it in each of its scopes at an occurrence, from an array or from a function that the worker calls at each occurrence, in the transaction that advances the cursor of the schedule.

- Graphile Worker, differs: a cron item enqueues one job ([source](https://github.com/graphile/worker/blob/v0.18.0/src/cron.ts#L94-L112)), and its documentation recommends few recurring tasks whose jobs create the jobs of individual users ([docs](https://github.com/graphile/worker/blob/v0.18.0/website/docs/cron.md#L28-L34)).
- Inngest, differs: a cron trigger starts one run of its function, and its guide has that function load the users and send an event for each, which runs a function of its own per user ([docs](https://github.com/inngest/website/blob/4fed28f96587641f115a92d8ff074f07d81b6491/pages/docs/guides/scheduled-functions.mdx#L23-L75)).
- Sidekiq, differs: a periodic job of Sidekiq Enterprise has static arguments evaluated at boot, and its documentation has a periodic job that reads rows with the ids of accounts and pushes a job for each ([docs](https://github.com/sidekiq/sidekiq/wiki/Ent-Periodic-Jobs/8914d061ac4276a97e3e4fcc4964037587582b1a)).
- pg-boss, differs: a schedule sends one job with its static data, and its `key` lets a queue have several schedules ([docs](https://github.com/timgit/pg-boss/blob/12.38.1/docs/api/scheduling.md#L143-L145)), so an application can register a schedule for each tenant.
- BullMQ, differs: a job scheduler creates one job of a fixed template at each iteration ([docs](https://github.com/taskforcesh/bullmq/blob/v6.7.5/docs/gitbook/guide/job-schedulers/README.md#L93-L98)), so an application can upsert a scheduler for each tenant.
- Solid Queue, differs: a recurring task enqueues one job with static arguments, and `SolidQueue.schedule_recurring_task` adds tasks at run time ([docs](https://github.com/rails/solid_queue/blob/v1.7.2/README.md#L955-L987)), so an application can schedule a task for each tenant.
- GoodJob, differs: a cron entry enqueues one job, whose `args`, `kwargs`, and `set` can be procs called at each enqueue ([docs](https://github.com/bensheldon/good_job/blob/v4.24.1/README.md#L823-L831), [source](https://github.com/bensheldon/good_job/blob/v4.24.1/app/models/good_job/cron_entry.rb#L243-L256)).
- Oban, lacks: an entry of the cron plugin inserts one job with its static args ([source](https://github.com/oban-bg/oban/blob/v2.24.2/lib/oban/cron.ex#L339-L377)).
