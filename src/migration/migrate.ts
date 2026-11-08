// Migration of the schema of kern-jobs by an application, and the check of the schema by a worker at its start.
import type { Database } from "kern-db"
import { getMigrationStatus, migrate, type MigrationTarget } from "kern-migrations"

import { checkSchemaName, DEFAULT_SCHEMA } from "../core/schema"

import { jobMigrations } from "./migrations"

/** Options of `migrateJobs`. */
export interface MigrateJobsOptions {
  /** PostgreSQL schema of the tables of kern-jobs, as of `createJobs` and `startWorker`; `kern_jobs` by default. */
  schemaName?: string | undefined
}

/**
 * Creates the PostgreSQL schema of kern-jobs with its tables, or updates the schema that an earlier version of kern-jobs created.
 * It runs on `target`: a driver of kern-db over a database client, such as `postgresJsDriver(sql)` of `kern-jobs/db/postgres-js`, as `startWorker` takes, a database of kern-db of the application, or a `Db` of kern-orm, whose own schema it ignores for `schemaName`.
 * Inside a transaction of the database, such as of a test, the migrations commit or roll back with that transaction.
 * It does nothing in a schema that is up to date, and processes that call it at once change the schema once.
 * It throws `TypeError` for a wrong target or option before it runs a statement.
 */
export async function migrateJobs(
  target: MigrationTarget,
  options: MigrateJobsOptions = {},
): Promise<void> {
  const { schemaName = DEFAULT_SCHEMA } = options
  checkSchemaName(schemaName)
  await migrate(target, jobMigrations, { schemaName })
}

/**
 * Checks that a schema has applied every migration of this version of kern-jobs, for a worker before it takes jobs.
 * It rejects with an `Error` that tells to call `migrateJobs` otherwise.
 * Migrations that a newer version of kern-jobs applied, such as during a rolling deploy, pass.
 */
export async function checkJobSchema(database: Database, schemaName: string): Promise<void> {
  const { pending } = await getMigrationStatus(database, jobMigrations, { schemaName })
  if (pending.length > 0) {
    throw new Error(
      `The schema "${schemaName}" of kern-jobs is not up to date: call migrateJobs of kern-jobs/migrations before the worker starts (pending migrations: ${pending.join(", ")})`,
    )
  }
}
