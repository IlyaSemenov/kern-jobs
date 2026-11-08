// Enqueues in the transactions of the ORMs that kern-jobs supports: Orchid ORM and kern-orm, as the documentation connects them, and runs kern-jobs in an application on kern-orm: migrations, enqueues, and a worker on one client.
import { afterEach, beforeEach, describe, expect, test } from "bun:test"

import { createJobs, defineTask, type JobsClient } from "kern-jobs"
import { postgresJsDriver as jobsPostgresJsDriver } from "kern-jobs/db/postgres-js"
import { migrateJobs } from "kern-jobs/migrations"
import { defineManifest, type Driver, startWorker } from "kern-jobs/worker"
import { bindSchema, createDatabase, defineSchema } from "kern-orm"
import { postgresJsDriver } from "kern-orm/db/postgres-js"
import { orchidORM } from "orchid-orm/postgres-js"
import postgres from "postgres"

import { createTestSchema, databaseUrl, type TestSchema } from "../testing/database"

/** ORM of an application with the client of `createJobs` of the documentation. */
interface Orm {
  jobsClient: JobsClient
  /** Runs `fn` in a transaction of the ORM, which commits when `fn` returns and rolls back when it throws. */
  transaction(fn: () => Promise<void>): Promise<void>
  close(): Promise<void>
}

const orms: [string, () => Orm][] = [
  [
    "Orchid ORM",
    () => {
      const db = orchidORM({ databaseURL: databaseUrl(), max: 2 }, {})
      return {
        jobsClient: () => db.$getAdapter(),
        transaction: fn => db.$transaction(fn),
        close: () => db.$close(),
      }
    },
  ],
  [
    "kern-orm",
    () => {
      const database = createDatabase({
        driver: postgresJsDriver(postgres(databaseUrl(), { max: 2 })),
      })
      return {
        jobsClient: bindSchema(defineSchema(), database),
        transaction: fn => database.transaction(fn),
        close: () => database.close(),
      }
    },
  ],
]

const indexProduct = defineTask("search.indexProduct")
  .withPayload<{ productId: string; version: number }>()
  .withOptions(({ productId }) => ({ key: `product:${productId}`, keyMode: "keep" }))

let schema: TestSchema
beforeEach(async () => {
  schema = await createTestSchema()
})
afterEach(async () => {
  await schema.dispose()
})

/** Returns the payloads of the jobs of the test schema in the order of their ids. */
async function payloads() {
  const { rows } = await schema.pool.query(`select payload from "${schema.name}".job order by id`)
  return rows.map(row => row.payload)
}

for (const [name, createOrm] of orms) {
  describe(name, () => {
    let orm: Orm
    beforeEach(() => {
      orm = createOrm()
    })
    afterEach(async () => {
      await orm.close()
    })

    const createTestJobs = () => createJobs(orm.jobsClient, { schemaName: schema.name })

    test("merges an enqueue outside a transaction into the pending job of its key", async () => {
      const jobs = createTestJobs()
      const first = await jobs.enqueue(indexProduct, { productId: "42", version: 1 })
      expect(first.created).toBe(true)
      expect(await jobs.enqueue(indexProduct, { productId: "42", version: 2 })).toEqual({
        id: first.id,
        created: false,
      })
      expect(await payloads()).toEqual([{ productId: "42", version: 1 }])
    })

    test("a job enqueued in a transaction appears when it commits", async () => {
      const jobs = createTestJobs()
      await orm.transaction(async () => {
        await jobs.enqueue(indexProduct, { productId: "42", version: 1 })
        expect(await payloads()).toEqual([])
      })
      expect(await payloads()).toEqual([{ productId: "42", version: 1 }])
    })

    test("a job enqueued in a transaction that rolls back never appears", async () => {
      const jobs = createTestJobs()
      const error = new Error("rollback")
      await expect(
        orm.transaction(async () => {
          await jobs.enqueue(indexProduct, { productId: "42", version: 1 })
          throw error
        }),
      ).rejects.toBe(error)
      expect(await payloads()).toEqual([])
    })
  })
}

describe("application on kern-orm", () => {
  /** Drivers of the worker: of the database itself, and of kern-jobs over the client of the database. */
  const workerDrivers: [string, (driver: Driver, sql: postgres.Sql) => Driver][] = [
    ["the driver of the database", driver => driver],
    ["a driver of kern-jobs/db/postgres-js over its client", (_, sql) => jobsPostgresJsDriver(sql)],
  ]

  for (const [name, createDriver] of workerDrivers) {
    test(`migrates and enqueues in one transaction of the ORM, and runs the job on ${name}`, async () => {
      const sql = postgres(databaseUrl(), { max: 3 })
      const driver = postgresJsDriver(sql)
      const database = createDatabase({ driver })
      const db = bindSchema(defineSchema(), database)
      // A schema of its own, since the migrations of the test create it.
      const schemaName = `${schema.name}_app`
      try {
        const jobs = createJobs(db, { schemaName })
        await database.transaction(async () => {
          await migrateJobs(db, { schemaName })
          await jobs.enqueue(indexProduct, { productId: "42", version: 1 })
        })

        const { promise: handled, resolve, reject } = Promise.withResolvers<unknown>()
        const worker = startWorker(createDriver(driver, sql), {
          manifest: defineManifest({ tasks: [indexProduct.implement(resolve)] }),
          schemaName,
          pollInterval: 0.05,
          onError: error => resolve(error),
        })
        // Before the timeout of the test, so that the cleanup below runs when the job never runs.
        const timer = setTimeout(() => reject(new Error("the job did not run within 3 s")), 3000)
        try {
          expect(await handled).toEqual({ productId: "42", version: 1 })
        } finally {
          clearTimeout(timer)
          await worker.stop()
        }
      } finally {
        try {
          await schema.pool.query(`drop schema if exists "${schemaName}" cascade`)
        } finally {
          await database.close()
        }
      }
    })
  }
})
