# Schedules

## Recovery of a recurring job

kern-jobs evaluates the current UTC minute, including at first registration, and can enqueue one job if any matching minute was missed within the configured window.
A current occurrence consumes that single enqueue.

- Graphile Worker differs: its regular loop rounds startup forward to the next minute, and bounded backfill queues each eligible missed occurrence of a known schedule ([source](https://github.com/graphile/worker/blob/v0.18.0/src/cron.ts#L365-L389), [backfill](https://github.com/graphile/worker/blob/v0.18.0/website/docs/cron.md#limiting-backfill)).
- pg-boss differs: it sends cron and RRULE occurrences due in the preceding 60 seconds.
  After downtime, the default `missed: 'skip'` skips earlier occurrences, and `missed: 'once'` can send one catch-up job alongside a currently due one ([scheduling](https://github.com/timgit/pg-boss/blob/12.38.1/docs/api/scheduling.md#catch-up-after-an-outage)).
