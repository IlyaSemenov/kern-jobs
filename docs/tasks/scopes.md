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

For a scoped task, `jobs.enqueue`, [`jobs.deletePending`](../workers/failed-jobs#delete-pending-jobs), and [`jobs.deleteFailed`](../workers/failed-jobs#delete-failed-jobs) resolve the scope on every call.
During a request of a tenant, enqueue the task as usual:

```ts
await jobs.enqueue(indexProductTask, { productId })
```

Keys and queues work within a scope: the jobs of different scopes with the same key never merge, and a queue of the same name does not make them run one at a time.
An unscoped job has no scope.
An enqueue of a task that is not scoped, such as of a cron job across all tenants, adds an unscoped job, also through `jobs` with a scope.

Outside of a request, such as in a script that enqueues a job for every tenant, pass the scope to `createJobs` as a string:

```ts
await createJobs(sql, { scope: tenant.id }).enqueue(indexProductTask, { productId })
```

To retry or delete the jobs of every scope, such as in a script of an operator, pass [`ALL_SCOPES`](../workers/failed-jobs#jobs-of-a-scope) instead.

A handler of a scoped task gets the scope as `job.scope`.
Every handler of a worker runs in one process with the same database clients, so open the data of the scope by `job.scope` in the handler, such as by the name of the schema of a tenant:

```ts [src/worker/tasks/index-product.ts]
import { indexProduct } from "../../search"
import { indexProductTask } from "../../tasks"

export default indexProductTask.implement(async ({ productId }, { job }) => {
  await indexProduct(job.scope, productId)
})
```

If your application keeps the current tenant in a context, such as an `AsyncLocalStorage` that the code of a request reads, pass `runHandler` to the worker to run every handler in the context of the scope of its job:

```ts [src/worker/start.ts]
import { findTenant, withTenant } from "../tenants"

const worker = startWorker(driver, {
  manifest,
  runHandler: async ({ job }, run) => {
    // A job of a task that is not scoped.
    if (job.scope === null) return await run()
    const tenant = await findTenant(job.scope)
    // The tenant was deleted: complete the job without running its handler.
    if (!tenant) return
    return await withTenant(tenant, run)
  },
})
```

[Wrap every handler](../workers/handlers#wrap-every-handler) tells how the outcome of `runHandler` settles the job.

To enqueue more jobs of the same scope from a handler, pass `scope: job.scope` to `createJobs`.

A worker does not run the handler for a job of a scoped task that has no scope, such as one enqueued before the task became scoped: it marks the job as failed at once, without retries, and does not run its [`onFailure`](../workers/failure-handlers) either.

A scope separates keys and queues, and the jobs that `jobs` retries and deletes, but nothing else: the jobs of all scopes share one table and the same workers, which run the jobs of every scope, so a scope neither limits the jobs of a tenant nor controls access to them.
A scope is at most 128 bytes long.
