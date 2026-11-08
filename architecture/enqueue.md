# Enqueue

A producer enqueues a job with one `INSERT … ON CONFLICT DO UPDATE` statement on its own connection, usually inside its transaction, so workers see the job when the transaction commits.
The statement returns the id of the row and whether it inserted the row.

A job without a key is always inserted.
A job with a key is inserted when no pending job of its `(scope, task, key)` exists, with `run_at` at the requested time, or now plus the requested delay, and `window_started_at` now.

## Key modes

When a pending job of the key exists, the enqueue updates it by its mode:

- `replace` sets the new payload, queue, and attempt budget, and `run_at = greatest(not_before, requested run_at)`.
  It keeps `attempts` unless the enqueue asks to reset them ([decision](../decisions/enqueue/replace-keeps-attempts.md)).
- `keep` leaves the payload, `run_at`, queue, and attempt budget of the row, but still locks the row until the producer commits ([decision](../decisions/enqueue/keep-locks-pending-row.md)).
- `debounce` updates the row as `replace` does, with `run_at` from [Debounce](#debounce).

Every mode sets `key_mode` of the row to its own, so the row carries the mode of its latest enqueue.
Keys are unique per scope and task ([decision](../decisions/enqueue/key-per-task.md)).

## Concurrency

- Concurrent producers of one key serialize on the entry of the unique index, and each computes its update from the committed row: 100 producers on 20 connections leave one row, and exactly one of them reports an insert, in each mode.
- A producer that waits for an uncommitted insert of the key merges into the row after the commit, or inserts its own row after a rollback.
- The pending row stays locked until the producer commits or rolls back, and claiming skips it meanwhile, so a job never starts before the transaction of its latest event commits.
- An active job is outside the uniqueness of keys: an enqueue during its run inserts the next pending row, including an enqueue that waited for an uncommitted claim of the pending row.

## Debounce

A debounced job runs `delay` seconds after the latest event, but no later than `maxWait` seconds after the first event of its window, when the events of the window pass the same `delay` and `maxWait`; the formulas give the rule for any settings:

```text
due:          least(now + delay, window_started_at + maxWait)
first event:  window_started_at = now, run_at = greatest(not_before, due)
next events:  run_at = greatest(not_before, run_at, due)
```

- A window opens at the insert of the row in `debounce` mode and at a debounce enqueue of a row whose latest enqueue had another mode, so an enqueue in another mode ends the window, and `maxWait` counts from the first debounce event rather than from an earlier `replace` or `keep` ([decision](../decisions/enqueue/debounce-window.md)).
- Within a window, `run_at` never moves backwards: it is the latest of the due times of the events of the window, each by its own `delay` and `maxWait`, so the order in which these events commit does not change it, a smaller `delay` or `maxWait` never makes the job due earlier, and a larger one extends the window.
- A claim takes the row out of the uniqueness of keys, so the next event inserts a new row and opens a new window.
- A failure returns the row to pending with a new window from the failure and `not_before` at the end of the backoff, below which no enqueue moves `run_at`; `maxWait` limits the wait of debounce, not of a retry, which can exceed the first event plus `maxWait` by up to the backoff.
- `maxWait` bounds the computed `run_at`, not the claim: locks of producers, busy workers, downtime, and retries can delay the claim further, which the claim lag shows ([decision](../decisions/enqueue/max-wait-bounds-run-at.md)).

For example, with `delay` 2 and `maxWait` 10, events every 0.5 seconds from 0 to 24.5 run the job at 10, 20.5, and 26.5.

## Accepted limitations

- Every enqueue on the key of a pending job writes a new row version, and `replace` and `debounce` usually move `run_at`, which the ready indexes contain, so their updates are not HOT and add index entries; vacuum reclaims them, and a sustained rate on one key, such as 1000 enqueues a second, is not measured.

## Open questions

- `keep` could lock the row with `ON CONFLICT DO UPDATE … WHERE false`, which writes no row version but returns no id.
