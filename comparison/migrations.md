# Migrations

## Migrating the schema

The application calls `migrateJobs` before its producers and workers start, and a worker only checks that its schema is up to date.

- graphile-worker-orchid, differs: the application can migrate the schema with `runMigrations` on the connection of Orchid ORM and in its current transaction, but the runner of Graphile Worker still migrates at its start ([docs](https://github.com/IlyaSemenov/graphile-worker-orchid/blob/v2.5.1/README.md#runmigrations)).
- Graphile Worker, differs: its runner migrates the schema at its start ([source](https://github.com/graphile/worker/blob/v0.18.0/src/lib.ts#L509-L512)).
- pg-boss, differs: `start` migrates the schema, and with `migrate: false` rejects a schema that needs a migration ([docs](https://github.com/timgit/pg-boss/blob/12.38.1/docs/api/constructor.md#migrate)).
