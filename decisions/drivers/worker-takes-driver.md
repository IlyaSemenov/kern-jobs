# The worker takes a driver, not the database of the application

`startWorker` takes a driver of kern-db, the one over which the application creates its `Database`, while `createJobs` and `migrateJobs` also take that `Database` or a `Db` of kern-orm:

```ts
await migrateJobs(db)
export const jobs = createJobs(db)
const worker = startWorker(driver, { manifest })
```

The worker runs the transactions of fail and a session of its heartbeats, which it keeps open for its life and replaces after a connection error, in a `Database` of its own, and reports the failures to release their connections to `onError`; none of it may join a transaction of the application.

## A driver that the database exposes

kern-db could expose the driver of a `Database`, so that `startWorker(db)` takes it from `db.$database.driver`, and an application passes one value to all three calls.
But the worker would run past the guarantees of the database of the application: a database of `test: true` rejects every statement outside a test transaction, while the worker would commit on its pool, and `close` of the database would close the driver under the transactions of the worker, which it does not see.
The application already holds the driver it creates the database over, so the option would only save it a variable.

## Leaving the transactions of the database

kern-db could run a callback outside the current transaction and session of a database, such as `database.detach(callback)`, and the worker would run on the database of the application through its coordinator: the heartbeat in a session, fail in transactions.
An application would then pass one value to all three calls, and a database of `test: true` would keep the worker from committing outside the test transactions.
But the worker could not run on such a database at all, so the tests of an application would need another database for it, and the failures to release its connections would go to `onReleaseError` of the application rather than to `onError` of the worker.
