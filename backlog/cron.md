# Cron

Schedules in five-field cron expressions, which every worker evaluates, with no elected leader.

Preliminary solution:

- A table of schedules; each worker computes the due moments of each schedule and takes a moment by a conditional update, `set last_run_at = $moment where last_run_at < $moment`, so that no worker needs to be elected.
- The worker that took the moment enqueues a `keep` job keyed by the schedule and the moment in the same transaction.
- A schedule for every scope is a task of the application that lists its scopes and enqueues the job in each one; a failure for one scope must not stop the others, so it needs the outcome of each enqueue.

Not decided: the time zone of an expression and whether missed moments are caught up, until a task needs them, such as every day at 9 in the time zone of a company.

Done when three workers enqueue one job a minute together, and after a stop of 10 minutes, they enqueue the jobs of the missed moments by the chosen catch-up.

Graphile Worker runs a crontab in UTC on each worker, and its table `known_crontabs` lets only the first worker queue a job for a scheduled moment, so identical crontabs on several workers queue one job ([docs](https://github.com/graphile/worker/blob/v0.18.0/website/docs/cron.md#distributed-crontab)).
Its backfill queues a job for every missed moment of a schedule within a given period, without an option to queue only one ([docs](https://github.com/graphile/worker/blob/v0.18.0/website/docs/cron.md#limiting-backfill)).
pg-boss takes cron and RRULE expressions with a time zone, `tz`, checks its schedules every 30 seconds, and sends at most one job a minute for each schedule across instances ([docs](https://github.com/timgit/pg-boss/blob/12.38.1/docs/api/scheduling.md)).
Its `missed: 'once'` sends one job for the most recent occurrence missed during an outage ([docs](https://github.com/timgit/pg-boss/blob/12.38.1/docs/api/scheduling.md#catch-up-after-an-outage)).
