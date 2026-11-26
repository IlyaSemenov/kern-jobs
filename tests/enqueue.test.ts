// Enqueues through the public API: the schema that `migrateJobs` creates, task definitions, and a transaction of the application.

import { afterEach, beforeEach, expect, test } from "bun:test"

import { createJobs, defineTask } from "kern-jobs"
import { nodePostgresDriver } from "kern-jobs/drivers/node-postgres"
import { migrateJobs } from "kern-jobs/migrations"
import { createDatabase, createStatement } from "kern-orm/db"
import type pg from "pg"

import { createTestSchema, type TestSchema } from "../testing/database"

const indexProduct = defineTask("search.indexProduct", { queue: "search" })
  .withPayload<{ productId: string }>()
  .withKey(({ productId }) => `product:${productId}`)

let testSchema: TestSchema
beforeEach(async () => {
  testSchema = await createTestSchema()
})
afterEach(async () => {
  await testSchema.dispose()
})

/** Runs `fn` in a transaction on a client of the pool, which rolls back when `rollback` is set. */
async function transaction(fn: (client: pg.PoolClient) => Promise<void>, rollback = false) {
  const client = await testSchema.pool.connect()
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
  const jobs = createJobs(() => current ?? testSchema.pool, { schemaName: testSchema.name })
  const select = `select task, key, queue, payload from "${testSchema.name}".job`
  await transaction(async client => {
    current = client
    const { created } = await jobs.enqueue(indexProduct, { productId: "42" })
    current = undefined
    expect(created).toBe(true)
    expect((await testSchema.pool.query(select)).rows).toEqual([])
  })
  expect((await testSchema.pool.query(select)).rows).toEqual([
    {
      task: "search.indexProduct",
      key: "product:42",
      queue: "search",
      payload: { productId: "42" },
    },
  ])
})

test("migrateJobs and createJobs use the schema kern_jobs by default", async () => {
  const database = createDatabase({ driver: nodePostgresDriver(testSchema.pool) })
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
