# PostgreSQL

## Supported versions

kern-jobs supports PostgreSQL 15 and newer.

The minimum follows the community support window rather than features: PostgreSQL 14 reaches its end of life on 2026-11-12.
The SQL needs nothing newer than identity columns of PostgreSQL 10, but the plans of the claim statements, which decide how many advisory locks they take, are verified only on PostgreSQL 15 and 18.
The optional `client_connection_check_interval` comes from PostgreSQL 14.

CI runs the tests on PostgreSQL 15 and 18, except the restart scenario, which starts its temporary cluster from the PostgreSQL binaries of the runner, whatever their version.

## Isolation

Producers must enqueue, and applications must retry and delete jobs, in `READ COMMITTED` transactions, and the pool of the worker must run its single statements at `READ COMMITTED`, since the statements rely on it to recheck a row that a concurrent transaction changed while they waited for its lock; at `REPEATABLE READ` and `SERIALIZABLE`, such a wait ends in a serialization failure, which kern-jobs neither handles nor tests.

## Transactions

- An enqueue runs in the transaction of the producer and keeps the pending row locked until it ends.
- A retry and a delete run in the transaction of the application, as an enqueue does, and keep the rows that they lock or change locked until it ends.
- Each claim statement runs in its own autocommit transaction, since it holds an advisory lock of its queue until the transaction ends.
- Fail runs one explicit transaction, started at `READ COMMITTED`, on a connection that it owns until the transaction ends.
- Complete, heartbeat, and each reaper statement are single autocommit statements.
- `migrateJobs` runs its migrations in transactions at `READ COMMITTED` on a connection that it reserves, or in the transaction of the `Database` that it is given ([Migrations](migrations.md#migratejobs)).

## Time

Every statement that depends on time takes it from `statement_timestamp()` of its own statement; the tests pass virtual time instead.
An enqueue inside a long transaction computes `run_at` and debounce windows from the start of its statement, not from the commit.

## Connection failures

After a connection error, the driver never returns the connection to the pool while it may be broken, and a later statement gets another one ([Drivers](drivers.md#contract)).
After a worker dies, its server backend finishes the current statement and commits it, unless `client_connection_check_interval` is set on the connection, which lets the server cancel the statement before its commit.
Workers and their reapers reconnect after a restart of the server, also in immediate mode, and workers retry the settlements that the restart cut.

## Implementation details relied on

- An enqueue tells an insert from an update by `xmax = 0` of the returned row.

## Open questions

- Whether an enqueue rejects a transaction at another isolation level, and how the single statements of the worker run at `READ COMMITTED` when `default_transaction_isolation` of the database is higher.
- Whether the worker sets `client_connection_check_interval` on its connections.
- Not tested: failover to a replica, half-open connections, `statement_timeout`, `lock_timeout`, and `idle_in_transaction_session_timeout` of application connections, PgBouncer in transaction mode, and partitioned tables.
