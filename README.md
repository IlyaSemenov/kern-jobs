# kern-jobs

Background jobs for TypeScript, stored in PostgreSQL.
Enqueue typed jobs in the same transaction as your data changes, using your own client or ORM.
Merge repeated enqueues, debounce bursts of updates, and run jobs one at a time per entity.
Schedule recurring jobs with cron, without duplicates across workers.

It works with postgres.js, node-postgres, Orchid ORM, kern-orm, kern-db, or [another client](https://ilyasemenov.github.io/kern-jobs/database-clients#other-clients) that runs parameterized SQL.

kern-jobs is in early development, and the API changes between minor versions.

See the [documentation](https://ilyasemenov.github.io/kern-jobs/) and the [comparison](https://ilyasemenov.github.io/kern-jobs/comparison) with Graphile Worker and pg-boss.

## Install

```sh
npm install kern-jobs postgres
```

## Usage

The examples import `sql`, your postgres.js client, from `src/db.ts`: `export const sql = postgres(process.env.DATABASE_URL!)`.

Migrate the schema of kern-jobs at every deploy, before your application and its workers start:

```ts
// src/migrate.ts
import { postgresJsDriver } from "kern-jobs/drivers/postgres-js"
import { migrateJobs } from "kern-jobs/migrations"

import { sql } from "./db"

await migrateJobs(postgresJsDriver(sql))
await sql.end()
```

Define each task with the type of its payload in a module that your application and the worker share:

```ts
// src/tasks.ts
import { defineTask } from "kern-jobs"

export const sendWelcomeEmailTask = defineTask("email.sendWelcome").withPayload<{
  userId: string
}>()
```

Enqueue a job in the transaction that writes its data: workers see the job once the transaction commits, and never when it rolls back.

```ts
// src/users.ts
import { createJobs } from "kern-jobs"

import { sql } from "./db"
import { sendWelcomeEmailTask } from "./tasks"

await sql.begin(async tx => {
  const [user] = await tx`insert into users (email) values (${email}) returning id`
  await createJobs(tx).enqueue(sendWelcomeEmailTask, { userId: user.id })
})
```

Implement the task and start a worker in a separate process:

```ts
// src/worker.ts
import { postgresJsDriver } from "kern-jobs/drivers/postgres-js"
import { defineManifest, startWorker, stopOnSignals } from "kern-jobs/worker"

import { sql } from "./db"
import { sendWelcomeEmailTask } from "./tasks"

const sendWelcomeEmail = sendWelcomeEmailTask.implement(async ({ userId }) => {
  await mailer.sendWelcome(userId)
})

const worker = startWorker(postgresJsDriver(sql), {
  manifest: defineManifest({ tasks: [sendWelcomeEmail] }),
})

stopOnSignals(worker, { close: () => sql.end() })
```

To run a job from start to end in one script, see [Getting Started](https://ilyasemenov.github.io/kern-jobs/getting-started#try-it).

## Kern libraries

kern-jobs is part of [Kern](https://ilyasemenov.github.io/kern-orm/ecosystem), a family of PostgreSQL libraries for TypeScript, alongside [kern-orm](https://ilyasemenov.github.io/kern-orm/) and [kern-db](https://ilyasemenov.github.io/kern-orm/database/getting-started).
