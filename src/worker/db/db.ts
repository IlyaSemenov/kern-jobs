// ORM schema for typed worker queries.
import { bindSchema, createTableFactory, type DbOf, defineSchema, toSnakeCase } from "kern-orm"
import type { Database } from "kern-orm/db"
import * as t from "kern-orm/types"

const { defineTable } = createTableFactory({ columnName: toSnakeCase })

// Declare only the columns used by typed worker queries.
const schema = defineSchema({
  job: defineTable("job", {
    id: t.bigint.primaryKey(),
    claimId: t.bigint.nullable(),
    state: t.text,
    scope: t.text,
    task: t.text,
    key: t.text.nullable(),
    keyMode: t.text,
    attempts: t.integer,
    maxAttempts: t.integer,
  }),
  schedule: defineTable("schedule", {
    id: t.text.primaryKey(),
    evaluatedThrough: t.timestamptz,
  }),
})

/** Worker database with its ORM schema bound. */
export type WorkerDb = DbOf<typeof schema>

/** Binds the worker ORM schema to `database` using `schemaName` for PostgreSQL names. */
export function bindWorkerDb(database: Database, schemaName: string): WorkerDb {
  return bindSchema(schema, database, { schemaName })
}
