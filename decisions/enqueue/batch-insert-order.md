# `enqueueMany` inserts in the order of its items

The statement of `enqueueMany` inserts its rows in the order of the items, so the identity of the table gives the new rows ids in that order, and a queue, which starts its jobs by `(run_at, id)`, starts those due at the same time in the order of the payloads, such as the events of one account.
It locks the keys in that order too, so two transactions that enqueue overlapping keys in different orders can deadlock, as single enqueues in transactions can; PostgreSQL detects the deadlock and aborts one of them with `40P01`.

## Keys in a sorted order

Inserting the rows by `(scope, task, key)` makes concurrent batches lock their keys in one order, so they do not deadlock with each other, though they still can with a transaction that holds a key from an earlier statement.
The identity then gives the new rows with a key ids in the order of their keys, so a queue starts them in that order rather than in the order of the payloads.

## Advisory locks of the keys in a sorted order

Taking a transaction-level advisory lock of a hash of each key in a sorted order before the insert, as BullMQ does for the deduplication ids of `addBulk` on PostgreSQL ([source](https://github.com/taskforcesh/bullmq/blob/v6.7.5/src/postgres/migrations/0008_bulk_dedup_lock_order.sql#L39-L50)), keeps the ids in the order of the items and serializes two batches of the same keys.
But each advisory lock takes an entry of the shared lock table until the transaction ends, which `max_locks_per_transaction` sizes: with its default of 64 and 100 connections, a transaction took 10,000 advisory locks and failed with `out of shared memory` at 20,000.
Like the sorted keys, the locks do not prevent a deadlock with a transaction that holds a key from an earlier statement.

## Ids in the order of the items, locks in the order of the keys

Taking the ids from the identity sequence by `nextval` in the order of the items and inserting the rows sorted by key with `OVERRIDING SYSTEM VALUE` gives both orders.
But an explicit `nextval` needs `USAGE` or `UPDATE` on the sequence, and the insert of an id needs `INSERT` on the column, while the identity of the table assigns an id without a privilege on its sequence: a role with only `INSERT` and `SELECT` on the table inserts a row and fails on `nextval` with `permission denied for sequence`.
Every producer would need privileges that an enqueue of one job does not.
