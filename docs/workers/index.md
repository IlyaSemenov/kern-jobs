# Running Workers

A worker runs the jobs of your tasks in the process that starts it.
Implement each task with the handler of its jobs, as the default export of a module of its own:

```ts [src/worker/tasks/index-product.ts]
import { indexProduct } from "../../search"
import { indexProductTask } from "../../tasks"

export default indexProductTask.implement(async ({ productId }) => {
  await indexProduct(productId)
})
```

List the implementations in a manifest:

```ts [src/worker/manifest.ts]
import { defineManifest } from "kern-jobs/worker"

import indexProduct from "./tasks/index-product"
import purgeExpiredSessions from "./tasks/purge-expired-sessions"

export const manifest = defineManifest({ tasks: [indexProduct, purgeExpiredSessions] })
```

Start a worker with the manifest and a driver of your database client:

```ts [src/worker/start.ts]
import { postgresJsDriver } from "kern-jobs/db/postgres-js"
import { startWorker } from "kern-jobs/worker"
import postgres from "postgres"

import { manifest } from "./manifest"

const sql = postgres(process.env.DATABASE_URL!)

const worker = startWorker(postgresJsDriver(sql), { manifest })
```

The worker takes the jobs of the tasks of its manifest as they become due, runs their handlers, and deletes each job whose handler returns.
A job whose handler throws [runs again later](./retries).

If your schema is not `kern_jobs`, pass its name as `schemaName`.

Create the schema with [`migrateJobs`](../getting-started#create-the-schema) before the worker starts: a worker takes no jobs in a schema that is missing or lacks the changes of its version of kern-jobs, and [reports](./monitoring#report-errors) it until the schema is up to date.

[Database Clients](../database-clients) shows how to start a worker on node-postgres or kern-orm, and [how many connections](../database-clients#connections-of-a-worker) it needs.

## Run several jobs at once

A worker calls its handlers as async functions in the process that started it, on the one thread of its event loop; it starts no threads or processes.
It runs one job at a time.
To run more, pass `concurrency`, the number of handlers that the worker keeps running at once:

```ts
const worker = startWorker(driver, { manifest, concurrency: 10 })
```

The handlers take turns as `Promise.all` would run them: while one awaits a query or a request, the others go on.
So `concurrency` speeds up handlers that wait, and does nothing for code that computes without `await`, such as resizing an image in JavaScript.
Such code stops every other handler and the worker itself: when it takes longer than the [lease](./leases), the leases of the running jobs expire, and another worker runs these jobs again.

- Move heavy computation out of the event loop, to a worker thread or a child process that the handler awaits.
- To use several CPU cores, start several processes with a worker in each: every job goes to one of them.

Give the client a connection for each handler that queries the database through it, besides the two of the worker, so that the handlers and the worker do not wait for one another:

```ts [src/worker/start.ts]
// 10 handlers that query through `sql`, the heartbeat of the worker, and the queries that take jobs and record their outcomes.
const sql = postgres(process.env.DATABASE_URL!, { max: 12 })

const worker = startWorker(postgresJsDriver(sql), { manifest, concurrency: 10 })
```

To limit a group of tasks on its own, such as heavy renders, start another worker with a manifest of these tasks and its own `concurrency`.
Each worker keeps one connection of its client, so give a client of several workers a connection for each of them and at least one more.

A worker looks for due jobs every second while it has free slots; pass `pollInterval` in seconds to change it.

## Check the manifest in tests

A job of a task that no worker implements stays pending until you [delete it](./failed-jobs#delete-a-job).
To catch a task that you defined and forgot to implement, compare the tasks of your application with its manifests in a test:

```ts
import { findUnimplementedTasks } from "kern-jobs/worker"

import * as tasks from "../src/tasks"
import { manifest } from "../src/worker/manifest"

test("every task has an implementation", () => {
  expect(findUnimplementedTasks(tasks, manifest)).toEqual([])
})
```

It takes the tasks among the exports of the module, and one manifest or an array of the manifests of all your workers.
