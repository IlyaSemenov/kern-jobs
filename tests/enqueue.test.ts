// Enqueues through the public API: the schema that `migrateJobs` creates, task definitions, and a transaction of the application.
import { afterEach, beforeEach, expect, test } from "bun:test"

import { createDatabase, createStatement } from "kern-db"
import { createJobs, defineTask } from "kern-jobs"
import { nodePostgresDriver } from "kern-jobs/db/node-postgres"
import { migrateJobs } from "kern-jobs/migrations"
import type pg from "pg"

import { createTestSchema, type TestSchema } from "../testing/database"

const indexProduct = defineTask("search.indexProduct")
  .withPayload<{ productId: string }>()
  .withOptions(({ productId }) => ({ key: `product:${productId}`, queue: "search" }))

let schema: TestSchema
beforeEach(async () => {
  schema = await createTestSchema()
})
afterEach(async () => {
  await schema.dispose()
})

/** Runs `fn` in a transaction on a client of the pool, which rolls back when `rollback` is set. */
async function transaction(fn: (client: pg.PoolClient) => Promise<void>, rollback = false) {
  const client = await schema.pool.connect()
  try {
    await client.query("begin")
    await fn(client)
  } finally {
    await client.query(rollback ? "rollback" : "commit").catch(() => {})
    client.release()
  }
}

test("a job enqueued in a transaction of the application appears when it commits", async () => {
  let current: pg.PoolClient | undefined
  const jobs = createJobs(() => current ?? schema.pool, { schemaName: schema.name })
  const select = `select task, key, queue, payload from "${schema.name}".job`
  await transaction(async client => {
    current = client
    const { created } = await jobs.enqueue(indexProduct, { productId: "42" })
    current = undefined
    expect(created).toBe(true)
    expect((await schema.pool.query(select)).rows).toEqual([])
  })
  expect((await schema.pool.query(select)).rows).toEqual([
    {
      task: "search.indexProduct",
      key: "product:42",
      queue: "search",
      payload: { productId: "42" },
    },
  ])
})

test("migrateJobs and createJobs use the schema kern_jobs by default", async () => {
  const database = createDatabase({ driver: nodePostgresDriver(schema.pool) })
  const jobs = createJobs(database)
  // In a transaction of the database that rolls back, so the test leaves no schema behind.
  const rollback = new Error("rollback")
  const outcome = database.transaction(async () => {
    await migrateJobs(database)
    const { id } = await jobs.enqueue(indexProduct, { productId: "42" })
    const { rows } = await database.execute(
      createStatement(`select task from kern_jobs.job where id = $1::text::bigint`, [id]),
    )
    expect(rows).toEqual([["search.indexProduct"]])
    throw rollback
  })
  await expect(outcome).rejects.toBe(rollback)
})
