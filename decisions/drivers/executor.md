# Enqueues through the client of the application

`createJobs` takes the client of the application, or a function that returns the client of the current transaction: an executor, an object with `query(text, values)` that resolves to `{ rows }`, or a client of postgres.js, whose `unsafe(text, values)` resolves to the rows:

```ts
createJobs(pool) // node-postgres: a pg.Pool, or a function that returns a pg.PoolClient
createJobs(() => tx ?? sql) // postgres.js
```

The statements on an executor read their rows by position and select only text, since the client of the application may rename columns and parse values: the enqueue returns one text column, a JSON array of the id and whether it inserted the row.

## Alternative

The executor could be a `Driver` of kern-db, as the worker takes, whose `execute(statement)` resolves to the command, count, columns, and rows as text:

```ts
createJobs(() => postgresJsDriver(tx ?? sql))
```

It has one contract for producers and workers, and gives results by the names of columns and errors classified as `QueryError` and `ConnectionError`.
But a producer would wrap each transaction object in the driver of its client; the driver of postgres.js rejects a client with `postgres.camel`, which applications use; and a client without a driver, such as that of an ORM with its own transactions, needs a new driver instead of one `query` function.
The executor keeps a statement that runs on it to positional text results, and gives the errors of the client unchanged.
