// Shared harness of the package tests: the test database, and a schema of kern-jobs per test, created by `migrateJobs` as applications create it.
import { nodePostgresDriver } from "kern-jobs/db/node-postgres"
import { migrateJobs } from "kern-jobs/migrations"
import pg from "pg"

/** Returns the URL of the test database, from `DATABASE_URL`. */
export function databaseUrl(): string {
  const url = process.env.DATABASE_URL
  if (!url) {
    throw new Error(
      "Set DATABASE_URL to a PostgreSQL database for tests, such as in .env.test.local.",
    )
  }
  return url
}

/** Schema of kern-jobs of a test, with a pool of the test database. */
export interface TestSchema {
  /** Name of the PostgreSQL schema. */
  name: string
  pool: pg.Pool
  /** Drops the schema and ends the pool. */
  dispose(): Promise<void>
}

/** Creates a schema of kern-jobs with a name of its own. */
export async function createTestSchema(): Promise<TestSchema> {
  const name = `t_${Math.random().toString(36).slice(2, 10)}`
  const pool = new pg.Pool({ connectionString: databaseUrl(), max: 2 })
  // The driver is not closed, since that would end the pool.
  await migrateJobs(nodePostgresDriver(pool), { schemaName: name })
  return {
    name,
    pool,
    async dispose() {
      try {
        await pool.query(`drop schema "${name}" cascade`)
      } finally {
        await pool.end()
      }
    },
  }
}
