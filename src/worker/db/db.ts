// Worker ORM schema, which each worker binds to its database.
import { bindSchema, type DbOf, defineSchema } from "kern-orm"
import type { Database } from "kern-orm/db"

import { job } from "./tables/job"
import { schedule } from "./tables/schedule"

const schema = defineSchema({ job, schedule })

/** Worker database with its ORM schema bound. */
export type WorkerDb = DbOf<typeof schema>

/** Binds the worker ORM schema to `database` using `schemaName` for PostgreSQL names. */
export function bindWorkerDb(database: Database, schemaName: string): WorkerDb {
  return bindSchema(schema, database, { schemaName })
}
