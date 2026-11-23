# Drivers

The worker talks to PostgreSQL through a driver of kern-db, which kern-jobs gives through the database entry points of kern-orm: `postgresJsDriver` of `kern-jobs/drivers/postgres-js` over a postgres.js 4 client or `nodePostgresDriver` of `kern-jobs/drivers/node-postgres` over a node-postgres 8 pool ([decision](../decisions/drivers/kern-db-drivers.md)).
[`migrateJobs`](migrations.md#migratejobs) takes such a driver too.
Enqueues of the public API run instead on the client of the application ([Producer](producer.md#client)).

`drivers/postgres-js.ts` and `drivers/node-postgres.ts` only re-export the drivers of kern-db.
`kern-jobs/worker` exports the type `Driver` of kern-db, so that an application names it without depending on kern-db.

`checkDriver` of `worker/driver.ts` throws `TypeError` for a value that `isDriver` of kern-db rejects, such as a client passed instead of its driver.

## Contract

kern-db keeps the [contract of its drivers](https://github.com/IlyaSemenov/kern-orm/blob/kern-db%400.4.0/architecture/drivers.md) and its tests, and the worker relies on it:

- A driver executes a statement in autocommit on any connection of its pool, or reserves a connection, which runs its statements one at a time in the order of calls, until it is released.
- It sends every parameter as text without a declared type and returns every value as text, so that the SQL alone decides the types, whatever parsers or serializers the client is configured with ([decision](../decisions/drivers/text-values.md)).
- A statement that PostgreSQL rejected throws `QueryError` with its SQLSTATE and constraint, and its connection stays usable; any other failure throws `ConnectionError`, after which the connection may be lost or in an unknown transaction state.
- A driver returns a released connection to the pool only when it is idle and usable, and closes it otherwise, such as one left in a transaction or lost.
- A connection lost inside a transaction rejects every later statement, so no statement of the transaction runs outside it on a new connection.
- The driver of postgres.js rejects a client that transforms column names or rows, since the core reads the columns of a result by their names.

## Statements

`kern-jobs/worker` loads kern-orm, and each worker binds the ORM schema of `worker/db/db.ts` to its database in the configured PostgreSQL schema at startup.
Worker ORM definitions use camelCase keys and map them to snake_case PostgreSQL columns with `toSnakeCase`.
Simple worker statements use typed table queries on this database, such as complete, the row locks and deletes of fail and requeue, and the registration, lock, and advance of the cursor of a schedule ([decision](../decisions/drivers/worker-orm.md)).
Operations that depend on the shape of a statement, including the locks and index scans of a claim, the terminal failure that inserts its failure job, the return to pending, and an enqueue, use hand-written SQL, which in fail, requeue, and the check of a schedule runs in the same transaction as their typed queries.
Producer statements use SQL that the application client can execute directly.

For raw SQL with named result columns, the worker runs its statements through `query` of `worker/db/query.ts`, which turns each parameter into text by `encodeParam`, from a number, boolean, date, or array of strings, and returns the rows as records by the names of their columns.
A statement that also runs on the executor of an application, such as an enqueue, runs through `queryValues` of `sql/statement.ts`, which returns the rows as arrays of values, since the client of the application may rename the columns ([Producer](producer.md#executor)).

- Each hand-written statement casts each parameter from text, as `$n::text::int`, since statements that run on connections of the application, such as an enqueue, meet clients that serialize a parameter by the type that PostgreSQL describes: postgres.js turns the string `"true"` of a `boolean` parameter into false and encodes a JSON string for `jsonb` again.
- Hand-written SQL names the job table and its claim sequence by `JobTable` of `sql/job-table.ts`, qualified by their PostgreSQL schema ([Data model](data-model.md)).
- Hand-written SQL returns a boolean as text, `(…)::text`, and job rows through `jobColumns`, which gives timestamps as seconds since the epoch, so that reading them depends neither on the client nor on `DateStyle` and `TimeZone`.
- Each statement that kern-jobs writes starts with its label in a comment, `/* kern-jobs:<area>:<statement> */`: hand-written SQL through `labelSql` and a typed query through `comment` with `toStatementLabel`, both of `sql/statement.ts`, whose `StatementOperation` lists the names, none of which is a prefix of another.
  Statements of kern-db and kern-migrations, such as `BEGIN`, the settings of a transaction, advisory locks, and migrations, carry none.
  A test finds a statement of kern-jobs by its label, followed by its hand-written SQL where it must tell the statement of its schema from those of others, and never by SQL that kern-orm generates.
- A typed worker query selects a timestamp as seconds since the epoch, `extract(epoch from …)`, since kern-orm decodes `timestamptz` only under the `DateStyle` `ISO`, which the pool of the application may not have.

## Connections of the worker

- Claims, complete, and the reaper run single statements on the pool.
- The check of the schema at the start of the worker runs its statements on the pool ([Migrations](migrations.md#check-of-a-worker)).
- Fail and requeue run in a transaction of a `Database` of kern-db that the worker creates over its driver, which reserves a connection from `BEGIN` to the end of the transaction, so a statement of another handler never runs inside it ([decision](../decisions/drivers/transaction-connection.md)).
  Scheduled enqueues start independent transactions in the same database; no application code runs transactions in it, so such a transaction never nests in another one, and the worker never closes it, since that would close the driver.
- Fail and requeue start their transaction at `READ COMMITTED`, whatever the default isolation of the connection.
- Heartbeats run in a session that the worker opens with `openSession` of its `Database`, which reserves a connection for the life of the worker from the pool of its driver, which therefore needs at least two connections, and one more for each other worker on it ([Worker](worker.md#accepted-limitations)); the runtime can take the connection from another pool, which tests use to break it alone.
  [Leases](leases.md#heartbeat) describes why they need a connection of their own.
  A heartbeat that PostgreSQL rejected keeps the session, and after a connection error, the worker closes it and opens another one.
  The session holds the lock of the worker id, which kern-db releases before it returns the connection to the pool ([Leases](leases.md#jobs-of-an-ended-worker)).
- The worker cancels no statement: the drivers of kern-db cannot, so a statement in flight when `stop` gives up ends in the background, bounded only by the timeouts of the client and the server ([Worker](worker.md#stopping)).
- The worker waits for the release of a connection, which a driver may finish asynchronously, and a failed release does not change the outcome of the statements on the connection: fail and requeue still return their outcome or throw their error, and the worker reports the failure as `release-error`.

A settlement whose operation lost its connection has an unknown outcome when [`isOutcomeUnknown`](https://github.com/IlyaSemenov/kern-orm/blob/kern-db%400.4.0/architecture/transactions.md#connection-failures) of kern-db says so, such as a fail or requeue transaction that lost it at `COMMIT` or a complete statement on the pool that was sent; the worker retries it either way ([Worker](worker.md#settlement)).

## Accepted limitations

- node-postgres gives the severity of an error only in the language of the server, so with translated messages, an error that ends the session with a code that a statement also gets, such as `40001` of a conflict with recovery on a replica, throws `QueryError`: the worker keeps the connection, and its next statement on it fails with `ConnectionError`.

## Open questions

- The single statements of the worker run at the default isolation of the connections of the pool.
