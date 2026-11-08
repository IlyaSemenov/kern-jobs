# Keys unique per scope and task

A key identifies the pending job of one task within a scope, so jobs of different tasks with the same key are independent, and a key needs no task prefix.
`replace` therefore never changes the task of a job.

The alternative is one key space per scope across tasks, as in Graphile Worker.
It lets a job of one task replace a pending job of another task with the same key.
With kern-jobs, the producer deletes the pending job of one task, such as by `jobs.delete` with the id that its enqueue returned, and enqueues the other in one transaction.
A delete by key is in the [backlog](../../backlog/administration.md).
Applications on Graphile Worker prefix their keys with task names to keep jobs of different tasks apart, which a per-task key space makes unnecessary.
