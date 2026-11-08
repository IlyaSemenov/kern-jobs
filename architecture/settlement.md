# Settlement

A worker settles a claimed job by complete, fail, or requeue.
Each matches the row by its id and claim id, so a stale claim settles nothing and learns that it lost the job.

## Complete

Complete deletes the active row of the claim in one statement and reports whether it deleted it.

## Fail

Fail runs one transaction on a connection that the driver reserves for it ([Drivers](drivers.md#connections-of-the-worker)):

1. It locks the active row of the claim; without one, the claim is lost, and nothing changes.
2. On a terminal error or on the last attempt, the row becomes `failed` with `failed_at` now and keeps its key.
3. A row without a key returns to pending with `run_at` and `not_before` at now plus the backoff, and a new debounce window.
4. A row with a key locks the pending row of its `(scope, task, key)`, the competitor, and then:
   - without a competitor, it returns to pending as in step 3;
   - with a competitor of key mode `keep`, the competitor is deleted, and the row returns to pending with its own attempts and backoff, so the first event still wins;
   - with a competitor of key mode `replace` or `debounce`, the row is deleted as superseded, since the competitor carries a newer event, and the competitor does not change.

The merge reads the key mode of the competitor, set by its latest enqueue, which during a rolling deploy comes from the newest producer code ([decision](../decisions/settlement/merge-on-failure.md)).
The worker computes the backoff from the retry policy of the task, and SQL receives it in seconds.

A producer can insert a competitor after step 4 finds none; then the return to pending fails with `23505`, and fail rolls the transaction back and retries it, at most five times.
A producer that waits for an uncommitted return to pending merges into the returned row after the commit.

Attempts belong to the row: a merge never moves them, and a terminal outcome is always recorded as `failed`, never deleted by a merge.

## Requeue

Requeue returns a job whose attempt does not count: the stop of its worker interrupted its handler, or began before the worker started it ([Worker](worker.md#stopping), [decision](../decisions/settlement/requeue-on-stop.md)).
It runs the transaction of fail with these differences:

- It never makes the row `failed`, also on its last attempt.
- The row returns to pending with `attempts` less one, `run_at`, `not_before`, and `window_started_at` at now, and its `last_error` kept.
- A competitor of its key merges as in fail: `keep` deletes the competitor, and `replace` or `debounce` deletes the row as superseded; a `23505` retries the transaction as in fail.

## Connection loss

Fail and requeue do not retry after a connection error: their transaction throws `TransactionConnectionError`, whose phase tells whether the server may have applied the transaction ([Drivers](drivers.md#connections-of-the-worker)).
The worker retries a settlement on a new connection while it owns the claim, which is safe since a settlement matches the claim id ([Worker](worker.md#settlement)).
A job whose settlement the worker gives up without a statement of unknown outcome stays active until the reaper returns it after its lease.
