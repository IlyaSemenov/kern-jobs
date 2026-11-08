# The worker takes a driver, not the database of the application

`startWorker` takes a driver of kern-db, the one over which the application creates its `Database`, while `createJobs` and `migrateJobs` also take that `Database` or a `Db` of kern-orm:

```ts
await migrateJobs(db)
export const jobs = createJobs(db)
const worker = startWorker(driver, { manifest })
```

The worker reserves a connection of the pool for its heartbeats for its life, replaces it after a connection error, and runs the transactions of fail in a `Database` of its own, whose `onReleaseError` it reports; none of it may join a transaction of the application.

## A driver that the database exposes

kern-db could expose the driver of a `Database`, so that `startWorker(db)` takes it from `db.$database.driver`, and an application passes one value to all three calls.
But the worker would run past the guarantees of the database of the application: a database of `test: true` rejects every statement outside a test transaction, while the worker would commit on its pool, and `close` of the database would close the driver under the transactions of the worker, which it does not see.
The application already holds the driver it creates the database over, so the option would only save it a variable.

## Leaving the transactions of the database

kern-db could run a callback outside the current transaction and session of a database, such as `database.detach(callback)`, and the worker would run on the database of the application through its coordinator: the heartbeat in a session, fail in transactions.
`close` of the database would then wait for the transactions of the worker, and a database of `test: true` would reject them.
But the session of the heartbeat lives as long as the worker, so `close` would wait until the worker stops, and the worker would rebuild on sessions what it does on a driver: replacing the connection of the heartbeat after an error, reporting failed releases, and taking the connection of the heartbeat from another pool in the tests of faults.
