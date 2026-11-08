# Backlog

Work not done yet, ideas, and research, one file per item, in buckets by importance.
Within Next and Later, items run roughly from the most important; the order is a loose hint, not a plan.

## Next

- [Worker events](worker-events.md): public events of the worker beside `onError`.
- [Cron](cron.md): schedules in cron expressions without an elected leader.
- [Enqueue from SQL](enqueue-from-sql.md): a SQL function for migrations of the application.
- [Testing helpers](testing-helpers.md): capturing enqueues and running handlers in tests.
- [Administration](administration.md): delete and check a key, act on many jobs at once, and count jobs.

## Later

- [Claiming with many queues](claiming-with-many-queues.md): the cost of a queue per entity, as Keys suggests.
- [Administration of scopes](administration-of-scopes.md): list and delete the jobs of a scope.
- [Scopes in the application context](scopes-in-the-application-context.md): a hook that runs handlers in the context of their scope.
- [Shared heartbeat connection](shared-heartbeat-connection.md): one heartbeat connection for the workers of a process.
- [Enqueue in an explicit transaction](enqueue-in-an-explicit-transaction.md): `jobs.using(client).enqueue(…)`.
- [Placement of the job table](placement-of-the-job-table.md): a table in a schema of the application or of each tenant, and partitions by scope.
- [Wakeup without polling](wakeup-without-polling.md): LISTEN/NOTIFY and a timer at the nearest `run_at`.
- [Transactional handlers](transactional-handlers.md): writes of a handler commit with the completion of its job.
- [Strict FIFO queues](strict-fifo-queues.md): a failed head blocks its queue.
- [Accumulating payloads](accumulating-payloads.md): a key mode that appends payloads to the pending job.
- [Batch enqueue](batch-enqueue.md): `enqueueMany` in one statement.
- [Payload validation](payload-validation.md): by a Standard Schema of the task.
- [Retention of completed jobs](retention-of-completed-jobs.md): keeping completed jobs for a configured time.

## Without a use case

Ideas that wait for an application that needs them, or that are not planned.

- [Priority](priority.md): claiming ready jobs by priority.
- [Rate limits](rate-limits.md): a limit of starts per period.
- [Group concurrency and batches](group-concurrency-and-batches.md): several active jobs per scope or queue, and batch handlers.
- [Job dependencies and workflows](job-dependencies-and-workflows.md): jobs that wait for other jobs.
- [Dashboard](dashboard.md): a user interface over administration.
- [Terminal failure hook](terminal-failure-hook.md): application writes in the transaction that fails a job for good.

## Keeping the backlog

- Give each item a file of its own with its preliminary decisions and open questions, and list it in this index under the bucket of its importance with a short summary.
- In Next and Later, add a new item where its importance roughly fits, without reordering other items to place it exactly.
  In Without a use case, add it at the end.
- Delete an item, its file and its line in this index, once it is done, moving its lasting contracts to [architecture/](../architecture/README.md), or once it is decided against, moving the decision to [decisions/](../decisions/README.md).
