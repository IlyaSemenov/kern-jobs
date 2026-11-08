# Migrations

The schema of kern-jobs, the job table with its claim sequence and its indexes ([Data model](data-model.md)), is created and changed by migrations of [kern-migrations](https://github.com/IlyaSemenov/kern-orm/blob/kern-migrations%400.4.1/architecture/migrations.md).
kern-jobs keeps their history and applies it itself, on a driver of kern-db, rather than give their SQL to the migration tool of the application ([decision](../decisions/migrations/own-history.md)).

## History

- `migration/migrations.ts` defines the history from loaders of the migrations of `migration/migrations/`, each the default export of a module named by its key, such as `0001_create_job`, so that a worker that checks the schema loads none of them.
- A migration builds its objects with the helpers of kern-migrations, and writes SQL only for what they lack.
- The history keeps its applied migrations in a table of its own name, `kern_jobs_migration`, in the schema of the jobs, so that it applies beside the history of an application in one PostgreSQL schema, whose table has the default name of kern-migrations.
- A migration names the table, the sequence, and the indexes without a PostgreSQL schema, which the search path of its transaction gives, so that the history applies to a schema of any name.
- A released migration never changes: a change of the schema is a new migration.
- A migration must keep the producers and the workers of the previous version of kern-jobs working, their enqueues, claims, heartbeats, settlements, and reaper, since they run on the migrated schema during a rolling deploy.
- A migration that builds or drops an index of a table that an earlier migration created must do it with `concurrently` of `createIndex` and `dropIndex` of kern-migrations, since a plain `CREATE INDEX` blocks the writes of every enqueue, claim, and settlement while it builds; a migration creates the indexes of its own new table with the table.
- Each migration has a `down` that restores the structure before it, for the verification of the history; kern-jobs has no API that rolls a schema back.

## migrateJobs

`migrateJobs` of `kern-jobs/migrations` applies the pending migrations of the history with `migrate` of kern-migrations to a schema, `kern_jobs` by default, and creates the schema when it is missing; `schemaName` of `createJobs` and `startWorker` must name the same schema.

- An application must call it before its producers enqueue and its workers start, at every deploy, since a worker does not migrate ([decision](../decisions/migrations/explicit-call.md)).
- Its target is a driver of kern-db, as `startWorker` takes, a `Database` of kern-db of the application, or a `Db` of kern-orm, whose own schema it ignores for `schemaName`.
  Inside a transaction of the database, the migrations commit or roll back with it.
- `migrate` gives the rest: the lock for processes that migrate at once, the transactions at `READ COMMITTED`, and a schema with migrations of a newer version of kern-jobs counted as up to date, so a process of the previous version that starts during a rolling deploy changes nothing.
- `migrateJobs` rejects with `TypeError` before it runs a statement if [`checkSchemaName`](producer.md#schema) rejects the schema name or the target is not a driver, a database, or a `Db`.
- A migration that fails rejects with the `MigrationError` of kern-migrations.

## Check of a worker

`checkJobSchema` of `migration/migrate.ts` is the check that a worker runs before its first claim ([Worker](worker.md#schema-check)).

- It rejects while the schema lacks a migration of the history, also a schema that does not exist, with an `Error` that names `migrateJobs` and the pending migrations.
- It resolves for a schema with migrations of a newer version of kern-jobs.
- It reads the table of versions with at most two statements on the pool and changes nothing.

## Dependencies

- kern-migrations and kern-db are dependencies of kern-jobs; kern-migrations requires kern-db as a peer, which the dependency of kern-jobs gives.
- `kern-jobs/migrations` and `kern-jobs/worker` load kern-migrations, and the main entry does not.

## Tests

- `verifyMigrations` of kern-migrations applies each migration of the history, rolls it back to the structure before it, and applies it again.
- `createTestSchema` and the harness of `testing/` create the schema of each test with the history, so every test of the core and the worker runs on the schema that `migrateJobs` creates.

## Open questions

- No test runs the producers and the workers of the previous version on a migrated schema, which the history of one migration cannot do; add it with the second migration.
- Removing the schema of kern-jobs has no API.
