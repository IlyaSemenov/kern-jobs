# Data model

All jobs live in one table, `job`, with its claim sequence `claim_seq`, in a PostgreSQL schema of their own, which the [migrations](migrations.md) create.
A job that completes is deleted; a job that fails for good stays as `failed` until the application retries or deletes it ([Administration](administration.md)).

Every statement names the table and the sequence qualified by their schema, so that it does not depend on the `search_path` of its connection, which an application may switch, such as per tenant; the [SQL function](enqueue.md#sql-function) `enqueue` binds the table at its creation for the same reason.

## Columns

- `task`: the name of the task, at most 128 bytes; a worker claims only the tasks it implements.
- `scope`: the scope, or the empty string for a job without one, at most 128 bytes.
- `queue`: the queue within the scope, at most 1024 bytes, or null.
- `key`: the key within the scope and task, at most 1024 bytes, or null.
- `key_mode`: `replace`, `keep`, or `debounce`, the mode of the latest enqueue of the row; [fail](settlement.md#fail) merges by it.
- `payload`: JSON, or null for a job enqueued without a payload.
- `run_at`: the earliest time at which a worker may claim the job.
- `not_before`: the lower bound of `run_at` after a failure, the end of its backoff.
- `window_started_at`: the start of the current debounce window: the insert, the latest return of the row to pending, or the latest debounce enqueue over a row of another key mode.
- `attempts`: the number of claims of the row, less the claims that a [requeue](settlement.md#requeue) gave back.
- `max_attempts`: the attempt budget, set by the enqueue, at least 1.
- `state`: `pending`, `active`, or `failed`.
- `claim_id`: the claim id, a new value of the sequence `claim_seq` at each claim.
- `lease_until`: the end of the lease.
- `locked_by` and `locked_at`: the worker and the time of the claim; a worker returns the active rows of its id at its start ([Leases](leases.md#jobs-of-an-ended-worker)).
- `last_error`: the error of the latest failure, `lease expired`, or `worker restarted`; a requeue and a retry keep it.
- `failed_at`: the time when the job became failed, by which the application deletes old failed jobs.
- `failure_max_attempts`: the stamp of the claim of an active row for a failure job, the attempt budget of that job, or null for a claim without one; inactive rows keep the stamp of their last claim, which nothing reads ([Failure jobs](failure-jobs.md)).
- `max_items`: the capacity of a batch job, whose payload is an array of at most that many items, or null for a job that is not a batch ([Batches](enqueue.md#batches)).
- `sealed`: whether a batch job takes no more items: an enqueue that fills it seals it, and so do an enqueue of another `max_items` and its first claim; the open batch of a key is its pending batch job that is not sealed ([decision](../decisions/enqueue/sealed-column.md)).

Check constraints enforce these invariants for every write:

- A row has each of `claim_id` and `lease_until` exactly while it is active.
- A row has `failed_at` exactly while it is failed.
- `attempts` is at least 0, and `max_attempts` at least 1.
- `run_at` is not below `not_before`, the end of the backoff.
- `max_items` is null or at least 1, and only a batch job is sealed; both constraints are `NOT VALID`, since the rows before their migration pass them, and they check every write.
- `failure_max_attempts` is null or at least 1, and set only for a task whose name is at most 120 bytes, so that the name of its failure task fits the limit of `task`; the constraint is `NOT VALID`, since the rows before its migration have null stamps, and it checks every write.
- `task`, `scope`, `queue`, and `key` are within their limits, which count bytes, since a character takes 1 to 4 bytes: they keep an entry of an index within the 2704 bytes of a B-tree entry also for names that do not compress.

## States

- A claim makes a pending job active.
- Complete deletes an active job.
- Fail makes an active job pending again, deletes it as superseded by the pending job of its key, or makes it failed; with a pending job of its key in mode `keep`, fail deletes that pending job instead ([Settlement](settlement.md#fail)).
- A fail or a return by the reaper that makes a job stamped for a failure job failed inserts that pending job ([Failure jobs](failure-jobs.md)).
- Requeue makes an active job pending again or deletes it as superseded, as fail does, but never makes it failed ([Settlement](settlement.md#requeue)).
- The reaper makes an active job whose lease expired pending again, or failed on its last attempt, and so does a worker at its start with the active jobs of its id.
- No worker or enqueue changes a `failed` row: it stays as a dead letter, outside the uniqueness of keys.
- A pending batch job is open until an enqueue fills it, an enqueue of another `max_items` seals it, or a claim seals it; every return of it to pending, by fail, requeue, the reaper, or a retry, keeps it sealed, with its id, payload, and key, so it never takes items again and never merges or detaches.
- A retry of the application makes a failed job pending again, unless it is not a batch job and a pending job of its key that is not sealed exists; a delete of the application deletes pending or failed jobs by id or by a filter ([Administration](administration.md)).

## Indexes

- `job_open_key_idx`, unique on `(scope, task, key)` of pending rows with a key that are not sealed, enforces the uniqueness of pending keys, beside which the sealed batch jobs of a key wait for a worker.
- `job_sealed_key_idx` on `(scope, task, key)` of pending sealed rows serves, beside it, the deletes of the pending jobs of a key.
- `job_active_queue_idx`, unique on `(scope, queue)` of active rows with a queue, enforces the queue mutex.
- `job_plain_ready_idx` on `run_at` of pending rows without a queue serves claiming them.
- `job_queue_ready_idx` on `(scope, queue, run_at, id)` of pending rows with a queue serves the queue scan and the queue head.
- `job_active_lease_idx` on `lease_until` of active rows serves the reaper.

The unique indexes, not the statements that check them, enforce the invariants of keys and queues, so a statement that acts on a stale snapshot fails with a unique violation (`23505`) instead of breaking an invariant, and the operation retries it ([decision](../decisions/claiming/queue-mutex-index.md)).

## Summary view

The view `job_summary`, which the migrations create beside the table, summarizes the jobs for the metrics of an application, so that the application reads neither the columns of the table nor its rows ([decision](../decisions/administration/summary-view.md)).

- It has a row for each `(scope, task)` that has jobs; `scope` and `task`, `text`, are the stored values, and `scope` is the empty string for an unscoped job.
- Its conditions and ages take `statement_timestamp()` of the statement that reads it, as the statements of the core do ([PostgreSQL](postgresql.md#time)).
- The counts, `bigint`, are:
  - `due`: the pending jobs whose `run_at` has come;
  - `scheduled`: the pending jobs whose `run_at` is ahead and whose `not_before` is null or has come;
  - `retrying`: the pending jobs whose retry backoff has not ended, `not_before` being ahead;
  - `running`: the active jobs;
  - `failed`: the failed jobs.

  Since `run_at` is not below `not_before`, `due`, `scheduled`, and `retrying` split the pending jobs.

- `oldest_due_seconds`, `longest_running_seconds`, and `oldest_failed_seconds`, `double precision` in seconds, give the time since the earliest `run_at` of the due jobs, `locked_at` of the active jobs, and `failed_at` of the failed jobs, or null without such a job; `longest_running_seconds` counts from the claim, so it includes the wait of an active job for its settlement or for the reaper.
- `most_attempts`, `integer`, is the greatest `attempts` of the pending and active jobs, which counts the claim of an active job, or null without such a job; failed jobs keep the attempts they spent until the application retries or deletes them, so they do not take part.
- Its columns, their types, units, and meanings, and the grouping of its rows are a public contract: a migration changes the view only by the rule of [Migrations](migrations.md#history).
- PostgreSQL checks the access to the table by the privileges of the owner of the view, the role that ran `migrateJobs`, so a role of the application needs only `USAGE` on the schema and `SELECT` on the view or on its columns.
- The summary of every task aggregates every row of the table.

## Invariants

Keys:

- Among pending rows that are not sealed, `(scope, task, key)` is unique; sealed, active, and failed rows do not take part.
- A batch job never has more than `max_items` items, and a key has at most one open batch.
- An enqueue while a job of the key is active creates the next pending row, and the active job keeps its key.
- No transition of a worker, an enqueue, or a retry drops a job silently: either the surviving row of its key absorbs it by the key mode, or the job loses its key and runs, or a sealed batch job returns with its own items; a delete of the application drops the job that it names.
- `attempts` of a row never decreases, except by an explicit reset in an enqueue, by a requeue, which gives back the attempt of its own claim, and by a retry, which resets it, and never moves to another row.
- No recomputation of `run_at` moves it below `not_before`.

Queues:

- At most one job of a `(scope, queue)` is active.
- A queue starts its ready jobs in the order of `(run_at, id)` in the snapshot of each claim, among the tasks that the claiming worker implements.

Claims:

- Every transition from `active` matches both `id` and `claim_id` of the claim, so a stale claim changes nothing.
- The claim id, not `lease_until`, decides ownership: `lease_until` only makes the row eligible for the reaper.
