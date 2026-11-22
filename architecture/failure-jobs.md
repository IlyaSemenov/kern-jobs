# Failure jobs

A task implementation may handle the terminal failures of its jobs: `task.implement(handler, { onFailure })`.
`implement` makes of `onFailure` the implementation of the failure task of the task, and when a claim of the implementation ends in a terminal failure, the statement that makes the job `failed` inserts one ordinary pending job of the failure task, the failure job, whose payload is the `JobFailure` of the failure ([decision](../decisions/settlement/failure-jobs.md)).

Every committed terminal failure of a claim stamped for a failure job inserts exactly one; the failure job then follows the contracts of any job: claiming, leases, settlement, retries, recovery, stopping, and administration.
This promises the creation of the work, not its success: the failure job can spend its own budget of attempts, also by crashes or late claims before `onFailure` starts, and `onFailure` can run more than once.

## Failure tasks

- The failure task of a task has its name with `:failure` after it, `FAILURE_TASK_SUFFIX` of `task/failure.ts`, which the insert and `toFailureTaskName` of `task/task.ts` share, and its `scoped`, since a failure job has the scope of the failed job, whose type the context of `onFailure` takes.
  It has the `maxAttempts`, as its option of an enqueue, and the backoff of an `onFailure` given as an object of them and `handler`, and none of the lease, the backoff, and the options of an enqueue of the task: without them, its jobs run under the lease of the worker and the default backoff, with the default `maxAttempts` of an enqueue, 25 ([decision](../decisions/settlement/failure-jobs.md#a-handler-of-the-implementation)).
- `implement` with `onFailure` returns the implementation with `failure`, the implementation of the failure task whose handler is `onFailure`; it throws the errors of a task name for the name of the failure task, so a task with `onFailure` has a name of at most 120 bytes.
- `defineTask` rejects a name that ends with `:failure`, so that a task of the application does not take the failure jobs of another by its name.
  An implementation built by hand with such a name is not told from a failure implementation, as one built with the name of any task of another worker is not.
- The types of `implement` give `onFailure` the `JobFailure` of the payload type of the task and the context of a handler of the task, and give the implementation a `failure` that `runJob` takes as an implementation.
- A failure task has no failure task of its own, so the terminal failure of a failure job inserts nothing: `implement` of a failure task and `resolveFailure` throw `TypeError` for an `onFailure` or a `failure` of it.
- A worker of an implementation with `failure` runs the failure task too: `toWorkerTasks` adds it under its name.
- A failure implementation is an implementation, so a manifest takes it on its own, such as of a worker that runs only the failure jobs of a task, or beside an implementation of the task without `onFailure`, whose claims stamp nothing, so that the failure jobs of an earlier release still run.
- The name of the failure task counts as a task name of a manifest, so another implementation of that name in the manifest, or in a manifest merged with it, throws as a duplicate task, and `findUnimplementedTasks` counts it as implemented.
- `toManifestTasks` of `task/manifest.ts` gives the tasks that a worker of a manifest runs, the task of each implementation followed by its failure task, from one read of the task and the failure implementation of each implementation, of the task of the failure implementation, and of the settings of both tasks, so that the manifest checks and `toWorkerTasks` check and run the same values.
  It runs `resolveFailure` of `task/task.ts`, which gives the failure task of an implementation, its checked name, and the attempt budget of its jobs, and throws `TypeError` for a `failure` of an implementation of a failure task, and for one without a handler or whose task has another name than the failure task of its task, and `RangeError` for a budget that is not a count; `toManifestTasks` itself throws `TypeError` for a failure task whose `scoped` differs from that of its task.

## Stamps

The claim stamps its row for a failure job, rather than an enqueue or a scan of failed rows, so that producers need not know which implementations have `onFailure`, and an `onFailure` applies to the jobs claimed after its deploy ([decision](../decisions/settlement/failure-jobs.md#enrollment)).

- The stamp is the attempt budget of the failure job, `failure_max_attempts`: the `maxAttempts` of the options of the failure task, or the default of an enqueue; it is a number of the row, since the worker or the reaper that inserts a failure job runs no code of the failure task, and the row needs no name of the failure task, which follows from its own.
- Each claim, of a job with or without a queue, overwrites `failure_max_attempts` of its row in the statement that makes it active, from the budgets of the claiming worker by task: the budget of the task, or null for a task without a failure task ([Claiming](claiming.md)).
- A claim rejects a stamp for a task whose name is longer than 120 bytes, by the constraint of the column, so that the name of the failure task fits the limit of `task` when the failure job is inserted.
- The stamp counts only on an active row, the one that its claim made active; nothing clears it in pending or failed rows, and the next claim replaces it, so the claim that ends in the failure decides whether it inserts a failure job.
- Ownership stays with the claim id: a stale claim settles nothing, so it inserts nothing.

## Insert

`insertFailureJobsSql` of `worker/db/failure.ts` gives the insert, a data-modifying CTE of the statement that makes the row `failed`, so the failure and the insert commit or roll back together, in every path that makes an active row `failed`:

- the terminal statement of [fail](settlement.md#fail), with the reason of the worker;
- the batch statement of the [reaper](leases.md#reaper), its per-row redo, and the return of the jobs of a worker of the same id, with `lease-expired` or `worker-restarted`.

It reads the stamp and the claim id of the row before the statement clears the claim, and inserts only for rows that the statement makes `failed`.

The failure job is a fresh pending job: the failure task of the task of the failed row, the stamped budget, the scope of the failed row, a null key and queue, the key mode `replace`, `attempts` 0, and `run_at`, `window_started_at`, and `created_at` at the `failed_at` of the failure.
A null key and queue cannot meet the unique indexes of pending keys and active queues, so the insert never conflicts, and no deduplication index is needed: only the transaction that commits the guarded transition of a claim from active inserts, a fail after an unknown commit finds the claim gone, and a statement that rolls back, such as a reaper batch before its per-row redo, inserts nothing.

An insert that PostgreSQL rejects rejects its statement, so the rows of that statement stay active: fail reports the rejection and retries it as a settlement, and the reaper reports the rejection as a failed round, which keeps the rows that its earlier statements returned, such as of a per-row redo ([Leases](leases.md#reaper)).

The worker decides the reason of a fail, never from `last_error`:

- `terminal-error`: a `TerminalError` of the handler or of `runHandler`, also before it calls the handler, and the rejection of a job of a scoped task without a scope; the worker passes it;
- `late-claim`: a claim that returned after the deadline of its lease on the last attempt, which the worker fails without running the handler; the worker passes it;
- `handler-error`: any other error of the handler or of `runHandler` on the last attempt, also an exhausted `RetryError`; the worker passes no reason, and the fail records this one.

An outcome that does not end in `failed` inserts nothing: a retry, a merge with the pending job of the key, a requeue by `stop`, a fenced settlement, and a return of the reaper to pending.

## `JobFailure`

The `JobFailure`, a type of the main entry, is built in SQL from the stored row, so it records what committed rather than what the handler holds:

- `id`: the claim id of the failure, as text, stable across the attempts of the failure job and unique within the installation; a retry of the application and a later failure give another one.
- `reason`: the reason of the failure.
- `error.message`: the `last_error` that the failure wrote; the stack, the cause, and the class of an error are not kept.
- `failedAt`: `failed_at` in ISO 8601 in UTC with milliseconds, truncated, by `to_char` of the time at UTC, independent of `TimeZone` and `DateStyle`.
- `job`: the id as text, the task, the scope with `null` for the empty scope, the attempt, `maxAttempts`, the key, and the queue of the failed row.
- `payload`: the stored payload, omitted only when it is SQL NULL, so a JSON `null` and the nulls inside it stay.
  The type gives it as the payload type of the task, required, since a field that is optional only for a payload type that admits `undefined` would make a task of some payload no `Task<string, unknown>` ([Producer](producer.md#task-definitions)).

## Consequences of ordinary jobs

- A failure job runs through the `runHandler` of its worker, with the name of the failure task and the `JobFailure` as the payload, and gets the errors of `onError` of any job.
- It takes the scoped-task check of its task: a failed job without a scope of a scoped task inserts an unscoped failure job, which fails terminally before `onFailure` runs, as the failed job did before its handler ([Worker](worker.md#tasks)).
- A failure job that no worker implements stays pending, such as after `onFailure` is removed from the implementation, or while only workers of a release without it run.
- A retry, a delete, a completion of the failed job, and retention neither change nor cancel its failure job; deleting a pending failure job races its claim like any delete.
- A failure job has no ordering against the jobs of the key or the queue of the failed job.

## Accepted limitations

- A failure job that spends its budget is not retried automatically; it stays failed under its task, and its terminal failure runs no handler.
- `onFailure` takes no lease of failure jobs, which run under the lease of the worker.
- A `maxAttempts` of `onFailure` applies to the failure jobs of the claims of the workers that have it, so the failure jobs of earlier claims keep theirs.
- A row whose failure job PostgreSQL always rejects stays active and fails the reaper batches that include it ([Leases](leases.md#accepted-limitations)).
