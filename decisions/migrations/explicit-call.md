# The application calls `migrateJobs`, and a worker only checks

An application calls `migrateJobs` before its producers and workers start, and a worker does not migrate: before its first claim, it checks that its schema is up to date, and reports a schema that is not until it is.

```ts
await migrateJobs(driver)

const worker = startWorker(driver, { manifest })
```

Producers enqueue through the executor of the application and cannot migrate, so only a step before every process of a new version makes the schema ready for all of them.
A migration must keep the previous version of kern-jobs working on the new schema; the new version need not support the previous schema.
A migration that fails rejects the call, which stops the deploy.

## Migrating at the start of a worker

An option of `startWorker`, such as `migrate: true`, would migrate before the first claim, with no call for the application to add:

```ts
const worker = startWorker(driver, { manifest, migrate: true })
```

Graphile Worker migrates at the start of its runner, on a client of its pool.

It covers the workers and not the producers: a producer of a new version that starts before the first worker of that version enqueues into the previous schema, or into none on an empty database, and its transaction fails.
So every migration would also keep the new version working on the previous schema, or the option would hold only where every process that enqueues starts a worker and waits for its migration.
`startWorker` is synchronous, so a migration that fails would go to `onError` while the worker keeps trying, and a process that also serves requests would have nothing to await before it serves them.
The role of the worker would need the rights to change the schema.
