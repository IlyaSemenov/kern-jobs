# The worker runs on the drivers of kern-db

The worker and `migrateJobs` take a driver of kern-db, and kern-db is a dependency of kern-jobs, which gives its drivers in subpaths:

```json
"dependencies": { "kern-db": "^0.4.0", "kern-migrations": "^0.4.1" }
```

```ts
import { postgresJsDriver } from "kern-jobs/db/postgres-js"
import { nodePostgresDriver } from "kern-jobs/db/node-postgres"
```

kern-db keeps the drivers and the tests of their contract, which covers what the worker relies on, such as the classes of errors and the release of connections, and the transactions that fail runs in.
An application installs kern-jobs and its database client, and imports the driver from kern-jobs, so the worker runs on a driver of the copy of kern-db that kern-jobs depends on.
The range is one minor line, since a minor version of kern-db may change the contract of its drivers before 1.0.

## Alternatives

kern-db could be a peer dependency, so that a package manager checks the copy of the application against the range of kern-jobs.
But every application would name kern-db, also one that only enqueues and never loads it, and import the driver from a package other than kern-jobs.

kern-jobs could keep drivers of its own in subpaths, with optional peer dependencies on their clients.
It would then change the contract of its drivers without a release of kern-db; kern-migrations and its peer kern-db would stay, for the migrations.
But it would maintain a copy of each driver and of the tests of their contract, about 800 lines, beside those of kern-db.

The worker could depend on one client, as Graphile Worker depends on `pg`, which needs no driver contract.
An application on postgres.js would then hold a second client library and pool for its worker.

## Cost

A change of the contract that the worker needs waits for a release of kern-db, and each minor version of kern-db needs a release of kern-jobs with its range.
A process that only enqueues installs kern-db, which the main entry does not load.

An application on kern-orm or kern-db may pass kern-jobs a value of another copy of kern-db: a driver of `kern-orm/db/postgres-js`, or its `Database` or `Db` to `migrateJobs`.
`isDriver` and `isDatabase` of kern-db accept such a value, and nothing checks that its contract matches the copy of kern-jobs.
Before 1.0, neither kern-jobs nor kern-db checks it, and no test covers mixed copies or installs with a single client: the packages are early prototypes without users, and these questions wait for 1.0.
