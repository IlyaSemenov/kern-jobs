# Database Clients

kern-jobs connects through the database client of your application in two ways:

- `createJobs` enqueues through the client itself, so that an enqueue joins the transaction that writes the data of the job.
  Pass it your client or the client of a transaction.
  You can also pass a function that returns the client of the current transaction: `createJobs` calls it at each enqueue.
- The worker and `migrateJobs` take a driver that wraps the client: `postgresJsDriver` of `kern-jobs/db/postgres-js`, or `nodePostgresDriver` of `kern-jobs/db/node-postgres`.
  They run outside the transactions of your application.

Enqueue, retry, and delete jobs in transactions at `READ COMMITTED`, the default isolation level of PostgreSQL.

kern-jobs never closes your client: close it yourself when your application stops, or in `close` of [`stopOnSignals`](./workers/stopping) in a worker.

## postgres.js

Pass your `sql` to `createJobs`:

```ts [src/jobs.ts]
import { createJobs } from "kern-jobs"
import postgres from "postgres"

export const sql = postgres(process.env.DATABASE_URL!)

export const jobs = createJobs(sql)
```

Start the worker on `postgresJsDriver(sql)`:

```ts [src/worker/start.ts]
import { postgresJsDriver } from "kern-jobs/db/postgres-js"
import { startWorker } from "kern-jobs/worker"

import { sql } from "../jobs"
import { manifest } from "./manifest"

const worker = startWorker(postgresJsDriver(sql), { manifest })
```

## node-postgres

Pass your `pg.Pool` to `createJobs`:

```ts [src/jobs.ts]
import { createJobs } from "kern-jobs"
import pg from "pg"

export const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL })

export const jobs = createJobs(pool)
```

Start the worker on `nodePostgresDriver(pool)`:

```ts [src/worker/start.ts]
import { nodePostgresDriver } from "kern-jobs/db/node-postgres"
import { startWorker } from "kern-jobs/worker"

import { pool } from "../jobs"
import { manifest } from "./manifest"

const worker = startWorker(nodePostgresDriver(pool), { manifest })
```

## kern-orm and kern-db

Pass your `db` to `createJobs`:

```ts [src/jobs.ts]
import { createJobs } from "kern-jobs"

import { db } from "./db"

export const jobs = createJobs(db)
```

An enqueue in `db.$transaction` then joins the transaction:

```ts
await db.$transaction(async () => {
  await db.product.find(productId).update({ price })
  await jobs.enqueue(indexProductTask, { productId })
})
```

Without the ORM, pass your `database`, and enqueue in `database.transaction`.

kern-jobs keeps its tables in its own schema, `kern_jobs`, whatever the schema of `db`.

Start the worker on the driver of your database, which shares its pool:

```ts [src/db.ts]
import { bindSchema, createDatabase } from "kern-orm"
import { postgresJsDriver } from "kern-orm/db/postgres-js"
import postgres from "postgres"

import { schema } from "./schema"

export const driver = postgresJsDriver(postgres(process.env.DATABASE_URL!))

export const database = createDatabase({ driver })

export const db = bindSchema(schema, database)
```

```ts [src/worker/start.ts]
import { startWorker, stopOnSignals } from "kern-jobs/worker"

import { database, driver } from "../db"
import { manifest } from "./manifest"

const worker = startWorker(driver, { manifest })

stopOnSignals(worker, { close: () => database.close() })
```

## Orchid ORM

Return the adapter of the current transaction, `db.$getAdapter()`:

```ts
import { createJobs } from "kern-jobs"

import { db } from "./db"

export const jobs = createJobs(() => db.$getAdapter())
```

An enqueue in `db.$transaction` then joins the transaction:

```ts
await db.$transaction(async () => {
  await db.product.find(productId).update({ price })
  await jobs.enqueue(indexProductTask, { productId })
})
```

## Transactions without an ORM

To enqueue in a transaction of postgres.js, pass `createJobs` the client of the transaction:

```ts
await sql.begin(async tx => {
  await tx`update product set price = ${price} where id = ${productId}`
  await createJobs(tx).enqueue(indexProductTask, { productId })
})
```

The job appears to workers when the transaction commits, and never when it rolls back.
With node-postgres, pass the `pg.PoolClient` that runs the transaction.

To share one jobs API across your application, such as `jobs` of `src/jobs.ts`, keep the client of the current transaction in an `AsyncLocalStorage`, and pass a function that returns it:

```ts
import { AsyncLocalStorage } from "node:async_hooks"

const transactionSql = new AsyncLocalStorage<postgres.TransactionSql>()

export const jobs = createJobs(() => transactionSql.getStore() ?? sql)

await sql.begin(tx =>
  transactionSql.run(tx, async () => {
    await tx`update product set price = ${price} where id = ${productId}`
    await jobs.enqueue(indexProductTask, { productId })
  }),
)
```

With node-postgres, keep the `pg.PoolClient` of the transaction instead, and return `transactionClient.getStore() ?? pool`.

## Other clients

Pass an object with a `query` function that runs the SQL text with its parameters `$1`, `$2`, … and resolves to the rows of the result as `rows`, each an object or an array of the values:

```ts
export const jobs = createJobs({ query: (text, values) => client.query(text, values) })
```

To enqueue in the current transaction of such a client, pass a function that returns the object instead.

The client may rename columns, such as with `postgres.camel`, and parse values of any type but `text` as it likes; keep `text` values as the database returns them.

## Connections of a worker

Give the client of a worker at least two connections: the worker keeps one of them while it runs, and takes the others for a moment to take jobs and to record their outcomes.
With one connection, the worker never becomes [ready](./workers/monitoring#check-the-health-of-a-worker) and takes no jobs.
The worker can share the client with the rest of your application, such as with the handlers and with `createJobs`; give the client a connection for each of the [handlers that run at once](./workers/#run-several-jobs-at-once) and query the database through it, besides the two of the worker.
