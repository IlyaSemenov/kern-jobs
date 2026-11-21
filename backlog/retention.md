# Retention

Deleting failed jobs after a configured time by the queue itself, without a scheduled task of the application, and keeping completed jobs for a configured time instead of deleting them.

## Use case

An application that moved to kern-jobs needs to delete its old failed jobs periodically.
It can [schedule a task](../docs/operations/managing-jobs.md#delete-old-failed-jobs-every-night) that calls `deleteFailed({ olderThan })`, and built-in retention would spare it that task.

## Preliminary

- Retention of failed and of completed jobs is one design: both delete rows of a state periodically by an age.
- A policy is off unless the application sets it.

## Open

- Whether retention is set per worker, per installation, such as in the table or by `migrateJobs`, or per task; with a setting of each worker, the shortest age among the workers that run the cleanup wins.
- What deletes the rows: a round of every worker, whose deletes repeat on each worker, or a schedule that the package defines, whose cursor keeps workers from enqueueing the same occurrence twice.
- What a completed job keeps until its retention ends, and from when its retention counts: complete deletes the row, and the table has neither a completed state nor a time of completion.
- Whether the delete needs an index of failed rows by `failed_at`, which the table lacks, since a frequent round scans the table.
- What the worker reports for a failed round, and how stopping ends it.
