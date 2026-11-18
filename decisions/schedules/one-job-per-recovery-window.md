# One job per recovery window

Enqueue one job when any unevaluated minute matches within the catch-up window or the current minute matches.
An hourly reconciliation that resumes at 03:00 after missing 01:00 and 02:00 scans the current state once.

## A job per missed occurrence

Graphile Worker can enqueue each eligible missed occurrence of a known schedule during [bounded backfill](https://github.com/graphile/worker/blob/v0.18.0/website/docs/cron.md#limiting-backfill).
That lets a task produce a report for each missing hour.
For a reconciliation that reads current state, each additional job repeats the scan.
