import { bindSchema, createTableFactory, type DbOf, defineSchema, toSnakeCase } from "kern-orm"
// ORM schema for typed worker queries.
import type { Database } from "kern-orm/db"
import { bigint, text } from "kern-orm/types"

const { defineTable } = createTableFactory({ columnName: toSnakeCase })

// Declare only the columns used by typed worker queries.
const schema = defineSchema({
  job: defineTable("job", {
    id: bigint.primaryKey(),
    claimId: bigint.nullable(),
    state: text,
  }),
})

/** Worker database with its ORM schema bound. */
export type WorkerDb = DbOf<typeof schema>

/** Binds the worker ORM schema to `database` using `schemaName` for PostgreSQL names. */
export function bindWorkerDb(database: Database, schemaName: string): WorkerDb {
  return bindSchema(schema, database, { schemaName })
}
