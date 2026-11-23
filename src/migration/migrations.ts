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
    "0002_schedule": () => import("./migrations/0002_schedule"),
    "0003_job_summary": () => import("./migrations/0003_job_summary"),
    "0004_failure_jobs": () => import("./migrations/0004_failure_jobs"),
    "0005_enqueue_function": () => import("./migrations/0005_enqueue_function"),
  },
  { tableName: MIGRATION_TABLE },
)
