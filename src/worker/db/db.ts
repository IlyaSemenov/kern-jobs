// Worker ORM schema, which each worker binds to its database.

import { bindSchema, type DbOf, defineSchema, defineSequence } from "kern-orm"
import type { Database } from "kern-orm/db"

import { job } from "./tables/job"
import { schedule } from "./tables/schedule"

/**
 * Worker ORM schema: the job and schedule tables and the claim sequence.
 * SQL of the worker names them through it, such as `${workerSchema.job}`, so that a statement of any `Db` of the schema names them in the PostgreSQL schema of that `Db`.
 */
export const workerSchema = defineSchema({
  job,
  schedule,
  /** Sequence of the claim ids. */
  claimSeq: defineSequence("claim_seq"),
})

/** Worker database with its ORM schema bound. */
export type WorkerDb = DbOf<typeof workerSchema>

/** Binds the worker ORM schema to `database` using `schemaName` for PostgreSQL names. */
export function bindWorkerDb(database: Database, schemaName: string): WorkerDb {
  return bindSchema(workerSchema, database, { schemaName })
}
