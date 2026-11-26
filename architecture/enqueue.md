# Enqueue

A producer enqueues a job with one `INSERT … ON CONFLICT DO UPDATE` statement on its own connection, usually inside its transaction, so workers see the job when the transaction commits.
The statement returns the id of the row and whether it inserted the row.
The items of a batch task take the SQL function `enqueue_items` instead ([Batches](#batches)).

A job without a key is always inserted.
A job with a key is inserted when no pending job of its `(scope, task, key)` that is not sealed exists, with `run_at` at the requested time, or now plus the requested delay, and `window_started_at` now.

## Key modes

When a pending job of the key exists, the enqueue updates it by its mode:

- `replace` sets the new payload, queue, and attempt budget, and `run_at = greatest(not_before, requested run_at)`.
  It keeps `attempts` unless the enqueue asks to reset them ([decision](../decisions/enqueue/replace-keeps-attempts.md)).
- `keep` leaves the payload, `run_at`, queue, and attempt budget of the row, but still locks the row until the producer commits ([decision](../decisions/enqueue/keep-locks-pending-row.md)).
- `debounce` updates the row as `replace` does, with `run_at` from [Debounce](#debounce).

Every mode sets `key_mode` of the row to its own, so the row carries the mode of its latest enqueue.
A pending batch of the key makes the enqueue of a job that is not a batch raise SQLSTATE `55000` in the function `check_merge` of the schema, which the update calls, so the statement changes nothing.
Keys are unique per scope and task ([decision](../decisions/enqueue/key-per-task.md)).

## Concurrency

- Concurrent producers of one key serialize on the entry of the unique index, and each computes its update from the committed row: 100 producers on 20 connections leave one row, and exactly one of them reports an insert, in each mode.
- A producer that waits for an uncommitted insert of the key merges into the row after the commit, or inserts its own row after a rollback.
- The pending row stays locked until the producer commits or rolls back, and claiming skips it meanwhile, so a job never starts before the transaction of its latest event commits.
- An active job is outside the uniqueness of keys: an enqueue during its run inserts the next pending row, including an enqueue that waited for an uncommitted claim of the pending row.

## Many jobs

`enqueueMany` of the core enqueues many jobs of one task and scope with one `INSERT … SELECT … ON CONFLICT DO UPDATE` statement, so they commit or roll back together, and merges each job with a key by its mode as an enqueue of one job does; one job takes the statement of one job, which costs less than parsing an array.

- The jobs travel as one text parameter, a JSON array, beside the task and the scope, so the text of the statement does not depend on their count; each payload is its JSON text of `encodePayload`, serialized as a value of its own as an enqueue of one job serializes it, before a target is needed.
- The jobs with a key must have distinct keys, since one statement cannot update a row twice, and PostgreSQL rejects such a batch with `21000`; the jobs API rejects such a call before the client ([decision](../decisions/enqueue/distinct-keys-in-many.md)).
- The statement inserts the rows in the order of the jobs, so the identity of the table gives the new rows ids in that order, and a queue starts them in that order at an equal `run_at`.
  It locks the keys in that order too, so transactions that enqueue overlapping keys in different orders can deadlock, and PostgreSQL aborts one of them with `40P01` ([decision](../decisions/enqueue/many-insert-order.md)).
- The update of a conflict reads `delay`, `maxWait`, and `resetAttempts` of its job, which are not columns of `excluded`, from a second parameter, a JSON object of the jobs with a key by their keys, which an uncorrelated subquery parses once per statement, also under a generic plan.
- The statement returns a row of each job in no promised order, and the core maps the rows back to the jobs: a job with a key by its key, and the jobs without a key to the new rows without a key in the ascending order of their ids.

## Batches

A batch is a job of a batch task, whose payload is an array of items that the enqueues of its key append, which `max_items` of the row bounds ([decision](../decisions/enqueue/batch-function.md)).
The core appends the items of an enqueue by one statement that calls the SQL function `enqueue_items` of the schema with its time, `$1`, so tests pass virtual time:

1. It checks its arguments by `check_enqueue` and `check_items`: a key for a non-empty array, a key mode other than `keep`, which would drop the items of the later enqueues, a JSON array, and `max_items` of at least 1.
2. Unless the array is empty, it takes the transaction-level advisory lock of its `(table, scope, task, key)`, so the batch enqueues of a key serialize until each transaction ends, as the lock of a pending row serializes the enqueues of its key.
3. One statement, whose snapshot is new since a function of SQL takes one for each statement, locks the open batch of the key, raises `55000` by `check_merge` for a pending job of the key that is not a batch, appends to it the items that fit, seals it when it is full, with `run_at` now but not below `not_before`, or otherwise computes its run time as a merge of its key mode, and inserts the rest in the order of the items: full jobs sealed and due now, and at most one partial job, open, with the run time and the debounce window of an insert.
   An open batch of another `max_items`, such as after a deploy that changes `maxItems`, is sealed with its items and run time, and every item goes to new jobs, so that a change of `maxItems` rejects no enqueue ([decision](../decisions/enqueue/resize-seals-open-batch.md)).
4. It returns a row of each job that took items, in the order of their items, the open batch first, and none for an empty array, which changes nothing; the core reads them by `WITH ORDINALITY`.

- The lock makes an enqueue that starts while another holds an uncommitted batch of the key wait and read the batch after the commit, so concurrent producers neither lose nor overfill items nor open two batches of a key, and append in the order in which they take the lock.
- A claim, which takes no such lock, skips the open batch that an enqueue holds, and an enqueue that waits for a claim of the open batch finds it active after the claim commits and opens a new batch.
- The open batch takes items after its run time until a claim makes it active; a claimed batch never takes items again ([Data model](data-model.md#states)).
- A full batch is due at once, also within its debounce window, and queues still serialize it.
- The update and the inserts of one call are one statement, which a rollback of the transaction undoes together.
- A job that is not a batch, whose producer takes no lock, can insert a pending job of the key between the snapshot and the insert of a partial batch, which then fails with the unique violation of `job_open_key_idx`; an enqueue of such a job that waits for an uncommitted batch of the key updates it after the commit, which `check_merge` rejects with `55000`, so an enqueue of either kind never merges into a job of the other.
- The fresh snapshot of each statement holds only at `READ COMMITTED`, at which producers must enqueue ([PostgreSQL](postgresql.md#isolation)): at a stricter level, an enqueue that waited for the lock reads the snapshot of its transaction, so it can miss the open batch that the other inserted and add a full batch beside it, or fail.
- The lock lasts until the transaction ends, so a long transaction that appended items holds the other appends of its key, as the lock of a pending row holds the other enqueues of its key.

## SQL function

The function `enqueue` of the schema enqueues one job from SQL, such as from a migration of the application, by a copy of the statement of one job of the core, and returns its `id` and `created` as `OUT` parameters, one row of each call.
The function `enqueue_items` appends items from SQL, as the core does, and returns a row of each job that took them.

- The arguments of `enqueue` are `task`, `payload`, and `scope`, and the options of an enqueue of the jobs API in snake case, `key`, `queue`, `key_mode`, `run_at`, `delay`, `max_wait`, `max_attempts`, and `reset_attempts`; only `task` is required.
  Those of `enqueue_items` are `task`, `items`, and `max_items`, which it requires, the same options, and `now`, the time of the enqueue, `statement_timestamp()` by default, which the core passes.
- An argument that is NULL takes its default, as an omitted one, so a column of a query that is NULL omits its option; `task` and `scope` reject NULL, and an omitted `scope` gives the empty scope, so that a scope that a query fails to find does not enqueue an unscoped job.
- `enqueue` checks its arguments as the jobs API checks the merged options, with the messages of the jobs API under the names of the arguments, and rejects a NULL `task` or `scope` with SQLSTATE `22004` and the following with `22023`: `delay` with `run_at`, `debounce` without `delay` and `max_wait` or with `run_at`, a delay outside 0 to 10^12 seconds, a `run_at` outside the years 1 to 9999, a `max_attempts` below 1, a `key_mode` of no mode, and the name of a [failure task](failure-jobs.md), which `defineTask` rejects; it ignores `max_wait` outside `debounce` and `reset_attempts` with `keep`; the check constraints of the table reject a value over its limit.
- It knows no task definition, so it applies neither the options of a task nor whether the task is scoped.
- Each call of `enqueue` runs its insert as a statement of its own in the transaction of the caller, with the [concurrency](#concurrency) of an enqueue, so the calls of one statement for the rows of a query enqueue one after another, and those of one key merge into one pending job, unlike `enqueueMany`.
- The time of `enqueue` is `statement_timestamp()`, the start of the statement of the caller, or of a query of several statements of a script, which every call of that statement shares.
- The body of `enqueue` is a SQL-standard body, which PostgreSQL parses at the creation of the function and binds to the table of the schema, so a call depends neither on the `search_path` of the caller nor on the name of the schema, which a rename may change ([decision](../decisions/enqueue/sql-function-binds-its-table.md)).
  A SQL-standard body has no `RAISE` statement, so the function `check_enqueue` of the schema, in PL/pgSQL, checks the arguments; it names no object of the schema, and its search path is fixed to `pg_catalog, pg_temp`, so its comparisons take the operators of `pg_catalog` whatever the search path of the caller.
- The functions are `SECURITY INVOKER`, so a caller needs `USAGE` on the schema, `EXECUTE` on the functions that it calls, `enqueue`, `enqueue_items`, `check_enqueue`, `check_items`, and `check_merge`, which PostgreSQL grants to `PUBLIC` for a new function unless the default privileges of its owner revoke it, and `SELECT`, `INSERT`, and `UPDATE` on the table; the statements of the core call `check_merge` and `enqueue_items` too.
- The statement of the core and the function `enqueue` are two copies, which a test compares on the same sequences of enqueues, retries, and claims, beside the same sequences of `enqueue_items` and of the core, which calls it ([decision](../decisions/enqueue/sql-function-copies-the-statement.md)): a change of the run time or the merge of a job must come with a migration that replaces the function.
- The body of `enqueue` depends on every column of the table through `excluded` of its `ON CONFLICT`, so a migration that adds or drops a column drops the function before and creates it after.

## Debounce

A debounced job runs `delay` seconds after the latest event, but no later than `maxWait` seconds after the first event of its window, when the events of the window pass the same `delay` and `maxWait`; the formulas give the rule for any settings:

```text
due:          least(now + delay, window_started_at + maxWait)
first event:  window_started_at = now, run_at = greatest(not_before, due)
next events:  run_at = greatest(not_before, run_at, due)
```

- A window opens at the insert of the row in `debounce` mode and at a debounce enqueue of a row whose latest enqueue had another mode, so an enqueue in another mode ends the window, and `maxWait` counts from the first debounce event rather than from an earlier `replace` or `keep` ([decision](../decisions/enqueue/debounce-window.md)).
- Within a window, `run_at` never moves backwards, until a batch fills, which ends its window and is due at once: it is the latest of the due times of the events of the window, each by its own `delay` and `maxWait`, so the order in which these events commit does not change it, a smaller `delay` or `maxWait` never makes the job due earlier, and a larger one extends the window.
- A claim takes the row out of the uniqueness of keys, so the next event inserts a new row and opens a new window.
- A failure returns the row to pending with a new window from the failure and `not_before` at the end of the backoff, below which no enqueue moves `run_at`; `maxWait` limits the wait of debounce, not of a retry, which can exceed the first event plus `maxWait` by up to the backoff.
- `maxWait` bounds the computed `run_at`, not the claim: locks of producers, busy workers, downtime, and retries can delay the claim further, which the claim lag shows ([decision](../decisions/enqueue/max-wait-bounds-run-at.md)).

For example, with `delay` 2 and `maxWait` 10, events every 0.5 seconds from 0 to 24.5 run the job at 10, 20.5, and 26.5.

## Accepted limitations

- Every enqueue on the key of a pending job writes a new row version, and `replace` and `debounce` usually move `run_at`, which the ready indexes contain, so their updates are not HOT and add index entries; vacuum reclaims them, and a sustained rate on one key, such as 1000 enqueues a second, is not measured.
- The JSON array of `enqueueMany` is one text parameter, which PostgreSQL limits to 1 GB, and the client holds it in memory.
  The statement of many jobs that all merge into pending jobs, in `debounce` mode with `resetAttempts`, took about 17, 190, and 970 ms for 1,000, 10,000, and 40,000 jobs under a generic plan; more jobs, and the serialization and the mapping of the results in JavaScript, are not measured.
- Each append to a batch writes the whole payload in a new row version, with its TOAST, so the cost of an append grows with the items of the open batch, which `max_items` bounds; a batch of thousands of items would need a table of items, and appends to one key are not measured.

## Open questions

- `keep` could lock the row with `ON CONFLICT DO UPDATE … WHERE false`, which writes no row version but returns no id.
