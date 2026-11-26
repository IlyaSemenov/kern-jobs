# Changing Tasks

kern-jobs does not migrate the jobs in the database when you change a task: a job keeps the task name and the payload of the release that enqueued it, and the type of the payload checks only the code.
During a deploy, the workers of each release take the jobs that the other one enqueued.
Until the deploy completes, the producers of the old release keep enqueuing in the old way.
So change a task in steps that both releases handle, and take the next step only after the deploy of the previous one completes:

- To add a field to the payload, add it as optional, pass it at every enqueue, and handle its absence in the handler.
  Make it required in a later release, once no job without it remains, including failed jobs that you may retry.
- To rename a task or change its payload in another way, such as to rename a field or to make it a batch task, define a task with a new name, such as `email.send.v2`, and enqueue it instead.
  Keep the implementation of the old task in the manifest until its jobs are gone: a worker takes only the jobs of the tasks of its manifest, so the jobs of a task that no worker implements stay pending without an error.
  The keys of the new task are its own: its enqueues do not merge into the pending jobs of the old task, so a job of each can run for one key.
- To remove a task, stop enqueuing it, and remove its implementation in a later release, once its jobs are gone.
  Instead of waiting, delete the jobs that remain by the task: the [pending ones](./managing-jobs#delete-pending-jobs) and the [failed ones](./managing-jobs#delete-failed-jobs).
  For a scoped task, delete them through `jobs` with [`ALL_SCOPES`](./managing-jobs#jobs-of-a-scope), which reaches the jobs of every scope.

The [failure jobs](../workers/failure-handlers) of a task have a name of their own, such as `email.send:failure`, so count them too before you [remove its `onFailure`](../workers/failure-handlers#add-and-remove-onfailure) or its implementation.

To see how many jobs of a task remain in each state, query the [summary of the jobs](./monitoring#watch-the-backlog) by its name:

```sql
select sum(due + scheduled + retrying) as pending, sum(running) as running, sum(failed) as failed
from kern_jobs.job_summary
where task = 'email.send'
```

The counts are null once no job of the task remains.
