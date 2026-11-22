# kern-jobs

Background jobs for TypeScript, stored in PostgreSQL: enqueue work in the transaction that writes your data, share typed task definitions between your application and its workers, and control retries and repeated updates.

It works with your PostgreSQL client: postgres.js, node-postgres, Orchid ORM, kern-orm, kern-db, or [another client](https://ilyasemenov.github.io/kern-jobs/database-clients#other-clients) that runs SQL text with parameters.

kern-jobs is in early development, and the API changes between minor versions.

See the [documentation](https://ilyasemenov.github.io/kern-jobs/), and how kern-jobs [compares](https://ilyasemenov.github.io/kern-jobs/comparison) with Graphile Worker and pg-boss.

## Install

```sh
npm install kern-jobs postgres
```

## Usage

The examples import `sql`, the postgres.js client of your application, from `src/db.ts`: `export const sql = postgres(process.env.DATABASE_URL!)`.

Create the schema of kern-jobs in your database before your application and its workers start, such as at every deploy:

```ts
// src/migrate.ts
import { postgresJsDriver } from "kern-jobs/drivers/postgres-js"
import { migrateJobs } from "kern-jobs/migrations"

import { sql } from "./db"

await migrateJobs(postgresJsDriver(sql))
await sql.end()
```

Define each task with the type of its payload in a module that both your application and the worker import:

```ts
// src/tasks.ts
import { defineTask } from "kern-jobs"

export const indexProductTask = defineTask("products.index").withPayload<{ productId: string }>()
```

Enqueue a job in the transaction that writes its data: workers see the job once the transaction commits, and never when it rolls back.

```ts
// src/products.ts
import { createJobs } from "kern-jobs"

import { sql } from "./db"
import { indexProductTask } from "./tasks"

await sql.begin(async tx => {
  await tx`update product set price = ${price} where id = ${productId}`
  await createJobs(tx).enqueue(indexProductTask, { productId })
})
```

Implement the task and start a worker in a process of its own:

```ts
// src/worker.ts
import { postgresJsDriver } from "kern-jobs/drivers/postgres-js"
import { defineManifest, startWorker, stopOnSignals } from "kern-jobs/worker"

import { sql } from "./db"
import { indexProductTask } from "./tasks"

const indexProduct = indexProductTask.implement(async ({ productId }) => {
  await updateSearchIndex(productId)
})

const worker = startWorker(postgresJsDriver(sql), {
  manifest: defineManifest({ tasks: [indexProduct] }),
})

stopOnSignals(worker, { close: () => sql.end() })
```

To run a job from start to end in one script, see [Getting Started](https://ilyasemenov.github.io/kern-jobs/getting-started#try-it).

## Kern libraries

kern-jobs is part of [Kern](https://ilyasemenov.github.io/kern-orm/ecosystem), a family of PostgreSQL libraries for TypeScript, and integrates with [kern-orm](https://ilyasemenov.github.io/kern-orm/) and [kern-db](https://ilyasemenov.github.io/kern-orm/database/getting-started) when your application uses them.
