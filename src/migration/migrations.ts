// Migrations of the schema of kern-jobs, which `migrateJobs` applies and a worker checks at its start.

import { defineMigrations } from "kern-migrations"

/** Name of the table of the applied migrations of kern-jobs, in the schema of the jobs. */
export const MIGRATION_TABLE = "migration"

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
    "0006_retry_function": () => import("./migrations/0006_retry_function"),
    "0007_batches": () => import("./migrations/0007_batches"),
  },
  { tableName: MIGRATION_TABLE },
)
