# Scopes

To keep the keys and queues of separate parts of your application apart, such as of tenants whose records have the same ids, give their jobs a scope.
Define the tasks of such jobs with `scoped: true`:

```ts [src/tasks.ts]
export const indexProductTask = defineTask("search.indexProduct", { scoped: true })
  .withPayload<{ productId: string }>()
  .withOptions(({ productId }) => ({ key: `product:${productId}` }))
```

Pass `createJobs` the scope of their jobs as a function that returns it, such as the tenant of the current request:

```ts [src/jobs.ts]
import { createJobs } from "kern-jobs"
import postgres from "postgres"

import { currentTenantId } from "./tenants"

export const sql = postgres(process.env.DATABASE_URL!)

export const jobs = createJobs(sql, { scope: () => currentTenantId() })
```

Each enqueue of a scoped task calls the function, so enqueue it as any other task, such as during a request of a tenant:

```ts
await jobs.enqueue(indexProductTask, { productId })
```

Keys and queues work within a scope: the jobs of different scopes with the same key never merge, and a queue of the same name does not make them run one at a time.
The jobs of the tasks that are not scoped, such as of a cron job across all tenants, have no scope, also when you enqueue them through `jobs` with a scope.

Outside of a request, such as in a script that enqueues a job for every tenant, pass the scope to `createJobs` as a string:

```ts
await createJobs(sql, { scope: tenant.id }).enqueue(indexProductTask, { productId })
```

A handler of a scoped task gets the scope as `job.scope`.
Every handler of a worker runs in one process with the same database clients, so open the data of the scope by `job.scope` in the handler, such as by the name of the schema of a tenant:

```ts [src/worker/tasks/index-product.ts]
import { indexProduct } from "../../search"
import { indexProductTask } from "../../tasks"

export default indexProductTask.implement(async ({ productId }, { job }) => {
  await indexProduct(job.scope, productId)
})
```

To enqueue more jobs of the same scope from a handler, pass `scope: job.scope` to `createJobs`.

A worker does not run the handler for a job of a scoped task that has no scope, such as one enqueued before the task became scoped: it marks the job as failed at once, without retries.

A scope separates only keys and queues: the jobs of all scopes share one table and the same workers, which run the jobs of every scope, so a scope neither limits the jobs of a tenant nor controls access to them.
A scope is at most 128 bytes long.
