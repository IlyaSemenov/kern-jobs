# Data model

All jobs live in one table, `job`, with its claim sequence `claim_seq`, in a PostgreSQL schema of their own, which the [migrations](migrations.md) create.
A job that completes is deleted; a job that fails for good stays as `failed` until the application retries or deletes it ([Administration](administration.md)).

Every statement names the table and the sequence qualified by their schema, so that it does not depend on the `search_path` of its connection, which an application may switch, such as per tenant.

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
- `locked_by` and `locked_at`: the worker and the time of the claim, for observation.
- `last_error`: the error of the latest failure or `lease expired`; a requeue and a retry keep it.
- `failed_at`: the time when the job became failed, by which the application deletes old failed jobs.

Check constraints enforce these invariants for every write:

- A row has each of `claim_id` and `lease_until` exactly while it is active.
- A row has `failed_at` exactly while it is failed.
- `attempts` is at least 0, and `max_attempts` at least 1.
- `run_at` is not below `not_before`, the end of the backoff.
- `task`, `scope`, `queue`, and `key` are within their limits, which count bytes, since a character takes 1 to 4 bytes: they keep an entry of an index within the 2704 bytes of a B-tree entry also for names that do not compress.

## States

- A claim makes a pending job active.
- Complete deletes an active job.
- Fail makes an active job pending again, deletes it as superseded by the pending job of its key, or makes it failed; with a pending job of its key in mode `keep`, fail deletes that pending job instead ([Settlement](settlement.md#fail)).
- Requeue makes an active job pending again or deletes it as superseded, as fail does, but never makes it failed ([Settlement](settlement.md#requeue)).
- The reaper makes an active job whose lease expired pending again, or failed on its last attempt.
- No worker or enqueue changes a `failed` row: it stays as a dead letter, outside the uniqueness of keys.
- A retry of the application makes a failed job pending again, unless a pending job of its key exists; a delete of the application deletes a pending or failed job ([Administration](administration.md)).

## Indexes

- `job_pending_key_idx`, unique on `(scope, task, key)` of pending rows with a key, enforces the uniqueness of pending keys.
- `job_active_queue_idx`, unique on `(scope, queue)` of active rows with a queue, enforces the queue mutex.
- `job_plain_ready_idx` on `run_at` of pending rows without a queue serves claiming them.
- `job_queue_ready_idx` on `(scope, queue, run_at, id)` of pending rows with a queue serves the queue scan and the queue head.
- `job_active_lease_idx` on `lease_until` of active rows serves the reaper.

The unique indexes, not the statements that check them, enforce the invariants of keys and queues, so a statement that acts on a stale snapshot fails with a unique violation (`23505`) instead of breaking an invariant, and the operation retries it ([decision](../decisions/claiming/queue-mutex-index.md)).

## Invariants

Keys:

- Among pending rows, `(scope, task, key)` is unique; active and failed rows do not take part.
- An enqueue while a job of the key is active creates the next pending row, and the active job keeps its key.
- No transition of a worker, an enqueue, or a retry drops a job silently: either the surviving row of its key absorbs it by the key mode, or the job loses its key and runs; a delete of the application drops the job that it names.
- `attempts` of a row never decreases, except by an explicit reset in an enqueue, by a requeue, which gives back the attempt of its own claim, and by a retry, which resets it, and never moves to another row.
- No recomputation of `run_at` moves it below `not_before`.

Queues:

- At most one job of a `(scope, queue)` is active.
- A queue starts its ready jobs in the order of `(run_at, id)` in the snapshot of each claim, among the tasks that the claiming worker implements.

Claims:

- Every transition from `active` matches both `id` and `claim_id` of the claim, so a stale claim changes nothing.
- The claim id, not `lease_until`, decides ownership: `lease_until` only makes the row eligible for the reaper.
