# Items append by a function of the schema

The core appends the items of a batch task by the SQL function `enqueue_items`, which checks its arguments, takes an advisory lock of the key by one statement, and by the next one reads the open batch of the key in a snapshot of its own, appends the items that fit, and inserts the rest.
A call is one statement of the caller, so it is atomic also in autocommit, and its lock serializes the appends to a key until each transaction ends.

```sql
create function enqueue_items(…) returns setof record language sql
begin atomic
  select check_enqueue(…);
  select check_items(…);
  select pg_advisory_xact_lock(hashtextextended(json_build_array('kern-jobs:batch', 'job'::regclass::oid, scope, task, key)::text, 0));
  with o as (select … from job where … and not sealed for update), u as (update job …), ins as (insert into job …)
  select …;
end
```

## One statement of the core

The core could append by one `INSERT … ON CONFLICT DO UPDATE` statement, as it enqueues a job of another task, with no function and no advisory lock.
Its update would append to the open batch the items that fit, by the row that the conflict locks, and a data-modifying CTE would insert the rest from its `RETURNING`.

- The rest depends on the length of the open batch before the update, which `RETURNING` gives only from PostgreSQL 18, by `old`; on PostgreSQL 15, `RETURNING` gives the row after the update, from which the length before it cannot be told when the update fills the batch.
- The insert of a statement whose snapshot misses the open batch of a concurrent enqueue cannot learn its length either: the conflict of its partial batch would have to merge items that it planned as new jobs, and could overfill the open batch.
- The statement could instead detect such a conflict, change nothing, and have the core run it again in a new snapshot, but an append from SQL has no loop, so it would need the lock anyway, and the two would diverge.

## A lock in one statement

The statement could take the advisory lock in a CTE, which needs no function.
A statement takes its snapshot before its lock, so after it waits for an enqueue that inserted the open batch of the key, it does not see that batch, and its partial batch fails with the unique violation of `job_open_key_idx` rather than append to it.

## A table of the open batches

A table of `(scope, task, key)` that each append upserts first would lock the key by its row instead of an advisory lock.
It adds a table, whose rows outlive their batches, and still needs a statement of a new snapshot after the lock to read the open batch, so it needs the function too.
