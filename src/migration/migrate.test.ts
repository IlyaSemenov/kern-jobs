// migrateJobs: creating the schema over a driver, in a transaction of a database, or with a Db, and its arguments; and the check of a schema by a worker.
// How kern-migrations applies a history, such as its lock, is tested there.
import { afterEach, beforeEach, describe, expect, test } from "bun:test"

import type { Driver } from "kern-db"
import { defineMigrations, getMigrationStatus, migrate } from "kern-migrations"
import { bindSchema, defineSchema } from "kern-orm"

import { enqueue } from "../core/enqueue"
import type { JobTable } from "../core/schema"
import { createTestSchema, jobs, type TestSchema } from "../core/testing"

import { checkJobSchema, migrateJobs, type MigrateJobsOptions } from "./migrate"
import { jobMigrations, MIGRATION_TABLE } from "./migrations"

let schema: TestSchema
/** Job table of a schema that the test migrates, beside the migrated schema of the test. */
let table: JobTable
beforeEach(async () => {
  schema = await createTestSchema()
  table = schema.unmigratedTable("target")
})
afterEach(() => schema.dispose())

/** Returns the keys of the migrations of kern-jobs that a PostgreSQL schema has applied, in their order. */
async function appliedKeys(name: string): Promise<readonly string[]> {
  return (await getMigrationStatus(schema.database, jobMigrations, { schemaName: name })).applied
}

/** Tells whether a PostgreSQL schema exists. */
async function schemaExists(name: string): Promise<boolean> {
  return (await schema.database.query`select from pg_namespace where nspname = ${name}`) === 1
}

describe("migrateJobs", () => {
  test("creates the schema with the job table over a driver", async () => {
    await migrateJobs(schema.driver, { schemaName: table.schemaName })
    const { id } = await enqueue(schema.admin, table, { task: "t" })
    expect((await jobs({ admin: schema.admin, table })).map(job => job.id)).toEqual([id])
    expect(await appliedKeys(table.schemaName)).toEqual(["0001_create_job"])
  })

  test("with a database, migrates in its transaction, which rolls the schema back", async () => {
    const rollback = new Error("rollback")
    const outcome = schema.database.transaction(async () => {
      await migrateJobs(schema.database, { schemaName: table.schemaName })
      await enqueue(schema.database, table, { task: "t" })
      throw rollback
    })
    await expect(outcome).rejects.toBe(rollback)
    expect(await schemaExists(table.schemaName)).toBe(false)
  })

  test("with a Db of kern-orm, migrates the schema of kern-jobs rather than the schema of the Db", async () => {
    const db = bindSchema(defineSchema(), schema.database, { schemaName: schema.name })
    await migrateJobs(db, { schemaName: table.schemaName })
    expect(await appliedKeys(table.schemaName)).toEqual(["0001_create_job"])
  })

  test("rejects a wrong target or option before it runs a statement", async () => {
    let calls = 0
    const call = () => {
      calls++
      return Promise.reject(new Error("a statement ran"))
    }
    const driver: Driver = { execute: call, executeScript: call, acquire: call, close: call }
    const cases: [unknown, unknown, string][] = [
      // A client instead of its driver.
      [{ query: call }, undefined, "The target of migrations must be"],
      [
        driver,
        { schemaName: 5 },
        "schemaName must be a string of 1 to 63 bytes in UTF-8 without U+0000",
      ],
      [driver, { schemaName: "" }, "schemaName"],
      [driver, { schemaName: null }, "schemaName"],
      // PostgreSQL would truncate it to 63 bytes.
      [driver, { schemaName: "a".repeat(64) }, "schemaName"],
    ]
    for (const [target, options, message] of cases) {
      const outcome = migrateJobs(target as Driver, options as MigrateJobsOptions)
      await expect(outcome).rejects.toThrow(message)
      expect(await outcome.catch((error: unknown) => error)).toBeInstanceOf(TypeError)
    }
    expect(calls).toBe(0)
  })
})

describe("checkJobSchema", () => {
  test("rejects a schema that lacks migrations with an error that names migrateJobs, and passes it once migrated", async () => {
    await expect(checkJobSchema(schema.database, table.schemaName)).rejects.toThrow(
      `The schema "${table.schemaName}" of kern-jobs is not up to date: call migrateJobs of kern-jobs/migrations before the worker starts (pending migrations: 0001_create_job)`,
    )
    await migrateJobs(schema.driver, { schemaName: table.schemaName })
    await checkJobSchema(schema.database, table.schemaName)
  })

  test("passes a schema with migrations of a newer version, as during a rolling deploy", async () => {
    const newerMigrations = defineMigrations(
      {
        "0001_create_job": () => import("./migrations/0001_create_job"),
        "0002_newer": { up: "select 1" },
      },
      { tableName: MIGRATION_TABLE },
    )
    await migrate(schema.database, newerMigrations, { schemaName: schema.name })
    await checkJobSchema(schema.database, schema.name)
  })
})
