// Migrations of the schema of kern-jobs, which `migrateJobs` applies and a worker checks at its start.
import { defineMigrations } from "kern-migrations"

/**
 * Name of the table of the applied migrations of kern-jobs.
 * It is a name of its own, so that the migrations apply beside those of an application in one PostgreSQL schema.
 */
export const MIGRATION_TABLE = "kern_jobs_migration"

/**
 * Migrations of the schema of kern-jobs, each the default export of a module of `migrations/` named by its key.
 * A module loads only when its migration applies, so a worker that checks the schema loads none.
 */
export const jobMigrations = defineMigrations(
  {
    "0001_create_job": () => import("./migrations/0001_create_job"),
  },
  { tableName: MIGRATION_TABLE },
)
