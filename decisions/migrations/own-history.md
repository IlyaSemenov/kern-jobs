# kern-jobs applies its own migrations

kern-jobs keeps the history of the migrations of its schema and applies it with kern-migrations on a driver of kern-db, the one that the worker runs on:

```ts
await migrateJobs(postgresJsDriver(sql))
```

The history records its applied migrations in the schema of the jobs.
So an upgrade of kern-jobs changes nothing in the application beside the call that it already makes, a worker can check that its schema is up to date, and one history applies to any schema, such as of each tenant, with the versions and the lock of that schema.
kern-migrations locks a schema for processes that migrate at once, accepts the migrations that a newer version applied during a rolling deploy, builds an index concurrently outside a transaction, and verifies that each migration rolls back.

The design gives up running the DDL on the client of the application: the migrations cannot join a transaction of a client other than a `Database` of kern-db, such as of the migration tool of the application, of a test that would roll the schema back, or of the code that creates a tenant with its schema.
An application on kern-db or kern-orm passes its `Database` instead of a driver, and the migrations join its transaction.

## SQL for the migration tool of the application

kern-jobs could return the SQL of its schema, and of each later change of it, which the application applies with its own migration tool:

```ts
await sql.unsafe(createSchemaSql())
```

The DDL then runs where the application runs its own: in its transactions and in the order of its migrations.
But the application copies the SQL of every change into a migration of its own at each upgrade of kern-jobs, and nothing records which changes a schema has, so a worker cannot tell a schema that lacks one.

## A migrator on the executor

kern-jobs could apply its history through the executor of the enqueues, which keeps one client for producers and migrations and joins the transactions of that client:

```ts
await migrateJobs(() => pool)
```

It would repeat what kern-migrations does, the table of versions, the lock, and the migrations of a newer version, on a contract of one statement with parameters.
A script of several statements and a transaction of its own are not in that contract, so the executor of every client would grow to give them, and an index could not be built concurrently in the transaction of the application.
