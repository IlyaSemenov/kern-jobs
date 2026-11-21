# A terminal failure inserts an ordinary job that runs `onFailure`

A terminal failure of a claim whose implementation has `onFailure` inserts one ordinary pending job of the failure task of its task in the statement that makes the job `failed`, with the `JobFailure` of the failure as its payload.
The failure job then runs `onFailure`, retries, and is recovered and administered as any job of its task.

## Alternatives

A callback in the worker that ran the handler, such as `sidekiq_retries_exhausted` of Sidekiq, `handle_error` of Que, or `after_discard` of Active Job, needs no table and gets the live error.
But it never runs for a job that fails without its handler: a crash, a lost lease, or a restart on the last attempt is recorded by the reaper or by another worker, which may not have the code of the task, and an error or a crash of the callback loses it.
A callback in the transaction that fails the job would roll the failure back on its error and still miss the reaper.

A table of its own for deliveries, beside the job table, would give a failure history that stays after the failure job succeeds, cancellation and retry of a delivery tied to the failed job, such as refusing to delete a failed job while its delivery is outstanding, and a scheduling policy of its own, such as unlimited attempts.
It needs its own claim, lease, heartbeat, settlement, recovery, fairness against ordinary jobs, administration, metrics, and test helpers: a second engine beside the one of jobs.

A state of the failed row, such as `failed` with a pending delivery, keeps one row per failure but mixes two lifecycles in one row: the claim, the attempts, and the error of the delivery would overwrite those of the failure, or need columns of their own, and claiming would scan failed rows.

The chosen design reuses the lifecycle, metrics, administration, and testing of jobs, and gives up the stronger distinctions of a table of its own:

- A failure job that succeeds is deleted, so no history of deliveries stays.
- A failure job has a finite budget, the `maxAttempts` of `onFailure` or the default of an enqueue, so crashes or late claims before `onFailure` starts can spend it, and the guarantee is the atomic creation of the work, not its success.
- A retry or a delete of the failed job does not cancel its failure job.

pg-boss chose the same shape: the statement that fails a job, also by expiration, inserts a copy into its dead-letter queue.

## A handler of the implementation

`onFailure` is a handler given to `implement`, alone or with the `maxAttempts` and the backoff of failure jobs, and `implement` makes the failure task of it: of the name of the task with `:failure` after it and of its `scoped`.

A failure task that the application defines and implements itself, given to `implement` as `onFailure: failedTask`, lets it choose the name and the lease of failure jobs, keep them out of the workers that run the task, and give a failure task a failure task of its own.
It costs every use three declarations for one handler, a task that no code enqueues, its implementation, and its entry in a manifest, and it needs checks that a handler does not: of the payload type and the scope of the failure task against the task, of failure tasks that no manifest implements, and of failure tasks that form a cycle, which no check sees across the releases of a deploy; and each row must store the name of its failure task beside the budget.
With a handler, the worker that runs a task with `onFailure` always runs its failure jobs, a failure task has no failure task, so no cycle exists, its scope is that of the task, and its name follows from that of the task.

Inngest has the same shape: `onFailure` of a function registers a separate function that the failure of the function triggers.

## Enrollment

The claim stamps its row for a failure job, from the implementation of the claiming worker, rather than an enqueue from the definition of the task, so that producers need not know which tasks have `onFailure` and adding one applies to the jobs already pending; and rather than workers scanning failed rows of the tasks that have `onFailure`, which would replay old failures when one is added and needs a scan of history.
The claim that ends in the failure decides, so an `onFailure` added or removed between attempts applies from the next claim, and the reaper, which runs no code of tasks, inserts the failure job from the stamp.
The stamp is the budget of the failure job, so that the reaper and the fail statement need no options of the failure task.
