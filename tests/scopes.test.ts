import { afterEach, beforeEach, expect, test } from "bun:test"
// Scopes of tenants through the public API: the jobs of a scoped task take the tenant of the current request, the jobs of tenants that share keys, queues, and the ids of their records stay apart, and a worker gives the handler and `onError` the scope of a job, by which the handler opens the data of its tenant.
import { AsyncLocalStorage } from "node:async_hooks"

import { createJobs, defineTask, type Task } from "kern-jobs"
import { nodePostgresDriver } from "kern-jobs/db/node-postgres"
import {
  defineManifest,
  type Driver,
  type Manifest,
  startWorker,
  TerminalError,
  type Worker,
  type WorkerErrorInfo,
} from "kern-jobs/worker"
import pg from "pg"

import { createTestSchema, databaseUrl, type TestSchema } from "../testing/database"

/** Options of the jobs of a product: one pending job and one running job per product. */
const productOptions = ({ productId }: { productId: string }) => ({
  key: `product:${productId}`,
  queue: `product:${productId}`,
})

const indexProduct = defineTask("search.indexProduct", { scoped: true })
  .withPayload<{ productId: string; version: number }>()
  .withOptions(productOptions)
const exportProduct = defineTask("catalog.exportProduct")
  .withPayload<{ productId: string; version: number }>()
  .withOptions(productOptions)

/** Tenant of the current request, as the application keeps it. */
const currentTenant = new AsyncLocalStorage<string>()

const tenants = ["a", "b"] as const

let schema: TestSchema
let driver: Driver
let worker: Worker | undefined
beforeEach(async () => {
  schema = await createTestSchema()
  driver = nodePostgresDriver(new pg.Pool({ connectionString: databaseUrl(), max: 3 }))
  // The data of each tenant in a schema of its own, with a product of the same id and its own title.
  for (const tenant of tenants) {
    await schema.pool.query(
      `create schema "${tenantSchema(tenant)}";
       create table "${tenantSchema(tenant)}".product (id text primary key, title text, indexed_title text);
       insert into "${tenantSchema(tenant)}".product (id, title) values ('42', 'title of ${tenant}')`,
    )
  }
})
afterEach(async () => {
  await worker?.stop()
  worker = undefined
  await driver.close()
  for (const tenant of tenants) {
    await schema.pool.query(`drop schema if exists "${tenantSchema(tenant)}" cascade`)
  }
  await schema.dispose()
})

/** Returns the PostgreSQL schema of the data of a tenant, as an application with a schema per tenant names it. */
function tenantSchema(tenant: string): string {
  return `${schema.name}_${tenant}`
}

/** Returns the jobs API of the application, whose scoped tasks take the tenant of the current request. */
function createAppJobs() {
  return createJobs(schema.pool, {
    schemaName: schema.name,
    scope: () => currentTenant.getStore()!,
  })
}

/** Starts a worker of the test schema, which polls fast, with the reports of its `onError`. */
function start(manifest: Manifest, concurrency = 1) {
  const reports: [unknown, WorkerErrorInfo][] = []
  worker = startWorker(driver, {
    manifest,
    schemaName: schema.name,
    concurrency,
    pollInterval: 0.02,
    onError: (error, info) => reports.push([error, info]),
  })
  return { worker, reports }
}

test("jobs of a scoped task in two tenants with the same key and queue stay apart, an enqueue merges only into the job of its tenant, and a task that is not scoped has no scope", async () => {
  const jobs = createAppJobs()
  const enqueueIn = (
    tenant: string,
    task: Task<string, { productId: string; version: number }>,
    version: number,
  ) => currentTenant.run(tenant, () => jobs.enqueue(task, { productId: "42", version }))
  const a = await enqueueIn("a", indexProduct, 1)
  const b = await enqueueIn("b", indexProduct, 1)
  const exported = await enqueueIn("a", exportProduct, 1)
  expect([a.created, b.created, exported.created]).toEqual([true, true, true])
  expect(await enqueueIn("a", indexProduct, 2)).toEqual({ id: a.id, created: false })
  const { rows } = await schema.pool.query(
    `select id, task, scope, key, queue, payload from "${schema.name}".job order by id`,
  )
  expect(
    rows.map(row => [row.id, row.task, row.scope, row.key, row.queue, row.payload.version]),
  ).toEqual([
    [a.id, "search.indexProduct", "a", "product:42", "product:42", 2],
    [b.id, "search.indexProduct", "b", "product:42", "product:42", 1],
    [exported.id, "catalog.exportProduct", "", "product:42", "product:42", 1],
  ])
})

test("a worker runs the jobs of one queue in two scopes at once, and each handler works on the data of the tenant of its scope", async () => {
  const jobs = createAppJobs()
  for (const tenant of tenants) {
    await currentTenant.run(tenant, () =>
      jobs.enqueue(indexProduct, { productId: "42", version: 1 }),
    )
  }
  const started = new Map<string, PromiseWithResolvers<void>>(
    tenants.map(tenant => [tenant, Promise.withResolvers<void>()]),
  )
  const bothStarted = Promise.all([...started.values()].map(s => s.promise))
  const indexed = Promise.withResolvers<void>()
  let pending = tenants.length
  const { worker, reports } = start(
    defineManifest({
      tasks: [
        indexProduct.implement(async ({ productId }, { job, signal }) => {
          started.get(job.scope)!.resolve()
          // Each handler waits for the other, so both run only if the queue does not serialize them across scopes; the abort ends the wait when it does, so that the test fails rather than hangs.
          const aborted = new Promise(resolve => signal.addEventListener("abort", resolve))
          await Promise.race([bothStarted, aborted])
          signal.throwIfAborted()
          await schema.pool.query(
            `update "${tenantSchema(job.scope)}".product set indexed_title = title where id = $1`,
            [productId],
          )
          if (--pending === 0) indexed.resolve()
        }),
      ],
    }),
    2,
  )
  await indexed.promise
  await worker.stop()
  const products = []
  for (const tenant of tenants) {
    const { rows } = await schema.pool.query(
      `select id, indexed_title from "${tenantSchema(tenant)}".product`,
    )
    products.push(rows)
  }
  expect(products).toEqual([
    [{ id: "42", indexed_title: "title of a" }],
    [{ id: "42", indexed_title: "title of b" }],
  ])
  const { rows } = await schema.pool.query(`select id from "${schema.name}".job`)
  expect([rows, reports]).toEqual([[], []])
})

test("onError gets the scope of the job whose handler threw", async () => {
  const { id } = await currentTenant.run("b", () =>
    createAppJobs().enqueue(indexProduct, { productId: "42", version: 1 }),
  )
  const thrown = Promise.withResolvers<void>()
  const error = new TerminalError("no product")
  const { worker, reports } = start(
    defineManifest({
      tasks: [
        indexProduct.implement(() => {
          thrown.resolve()
          throw error
        }),
      ],
    }),
  )
  await thrown.promise
  await worker.stop()
  expect(reports).toEqual([
    [
      error,
      {
        source: "handler",
        task: "search.indexProduct",
        job: {
          id,
          scope: "b",
          attempt: 1,
          maxAttempts: 25,
          key: "product:42",
          queue: "product:42",
        },
        aborted: null,
      },
    ],
  ])
})
