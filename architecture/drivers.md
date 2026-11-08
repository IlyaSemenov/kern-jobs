# Drivers

The worker talks to PostgreSQL through a driver of kern-db, which kern-jobs gives from its dependency on kern-db: `postgresJsDriver` of `kern-jobs/db/postgres-js` over a postgres.js 4 client or `nodePostgresDriver` of `kern-jobs/db/node-postgres` over a node-postgres 8 pool ([decision](../decisions/drivers/kern-db-drivers.md)).
[`migrateJobs`](migrations.md#migratejobs) takes such a driver too.
Enqueues of the public API run instead on the client of the application ([Producer](producer.md#client)).

`db/postgres-js.ts` and `db/node-postgres.ts` only re-export the drivers of kern-db.
`kern-jobs/worker` exports the type `Driver` of kern-db, so that an application names it without depending on kern-db.

`checkDriver` of `core/driver.ts` throws `TypeError` for a value that `isDriver` of kern-db rejects, such as a client passed instead of its driver.

## Contract

kern-db keeps the [contract of its drivers](https://github.com/IlyaSemenov/kern-orm/blob/kern-db%400.4.0/architecture/drivers.md) and its tests, and the worker relies on it:

- A driver executes a statement in autocommit on any connection of its pool, or reserves a connection, which runs its statements one at a time in the order of calls, until it is released.
- It sends every parameter as text without a declared type and returns every value as text, so that the SQL alone decides the types, whatever parsers or serializers the client is configured with ([decision](../decisions/drivers/text-values.md)).
- A statement that PostgreSQL rejected throws `QueryError` with its SQLSTATE and constraint, and its connection stays usable; any other failure throws `ConnectionError`, after which the connection may be lost or in an unknown transaction state.
- A driver returns a released connection to the pool only when it is idle and usable, and closes it otherwise, such as one left in a transaction or lost.
- A connection lost inside a transaction rejects every later statement, so no statement of the transaction runs outside it on a new connection.
- The driver of postgres.js rejects a client that transforms column names or rows, since the core reads the columns of a result by their names.

## Statements

The core runs its statements through `query` of `core/statement.ts`, which turns each parameter into text by `encodeParam`, from a number, boolean, date, or array of strings, and returns the rows as records by the names of their columns.
A statement that also runs on the executor of an application, such as an enqueue, runs through `queryValues`, which returns the rows as arrays of values, since the client of the application may rename the columns ([Producer](producer.md#executor)).

- Every statement casts each parameter from text, as `$n::text::int`, since statements that run on connections of the application, such as an enqueue, meet clients that serialize a parameter by the type that PostgreSQL describes: postgres.js turns the string `"true"` of a `boolean` parameter into false and encodes a JSON string for `jsonb` again.
- A statement names the job table and its claim sequence by `JobTable` of `core/schema.ts`, qualified by their PostgreSQL schema ([Data model](data-model.md)).
- A statement returns a boolean as text, `(…)::text`, and job rows through `jobColumns`, which gives timestamps as seconds since the epoch, so that reading them depends neither on the client nor on `DateStyle` and `TimeZone`.

## Connections of the worker

- Claims, complete, and the reaper run single statements on the pool.
- The check of the schema at the start of the worker runs its statements on the pool ([Migrations](migrations.md#check-of-a-worker)).
- Fail and requeue run in a transaction of a `Database` of kern-db that the worker creates over its driver, which reserves a connection from `BEGIN` to the end of the transaction, so a statement of another handler never runs inside it ([decision](../decisions/drivers/transaction-connection.md)).
  No other code runs transactions in that database, so such a transaction never nests in another one, and the worker never closes it, since that would close the driver.
- Fail and requeue start their transaction at `READ COMMITTED`, whatever the default isolation of the connection.
- Heartbeats run on a connection that the worker reserves for its life from the pool of its driver, which therefore needs at least two connections, and one more for each other worker on it ([Worker](worker.md#accepted-limitations)); the runtime can take the connection from another pool, which tests use to break it alone.
  [Leases](leases.md#heartbeat) describes why they need a connection of their own.
  A heartbeat that PostgreSQL rejected keeps the connection, and after a connection error, the worker releases it and reserves another one.
- The worker cancels no statement: the drivers of kern-db cannot, so a statement in flight when the timeout of `stop` passes ends in the background, bounded only by the timeouts of the client and the server ([Worker](worker.md#stopping)).
- The worker waits for the release of a connection, which a driver may finish asynchronously, and a failed release does not change the outcome of the statements on the connection: fail and requeue still return their outcome or throw their error, and the worker reports the failure as `release-error`.

A settlement whose operation lost its connection has an unknown outcome when [`isOutcomeUnknown`](https://github.com/IlyaSemenov/kern-orm/blob/kern-db%400.4.0/architecture/transactions.md#connection-failures) of kern-db says so, such as a fail or requeue transaction that lost it at `COMMIT` or a complete statement on the pool that was sent; the worker retries it either way ([Worker](worker.md#settlement)).

## Accepted limitations

- node-postgres gives the severity of an error only in the language of the server, so with translated messages, an error that ends the session with a code that a statement also gets, such as `40001` of a conflict with recovery on a replica, throws `QueryError`: the worker keeps the connection, and its next statement on it fails with `ConnectionError`.

## Open questions

- The single statements of the worker run at the default isolation of the connections of the pool.
