// Runs the worker of the consumer on the drivers of postgres.js and node-postgres, each in a schema of its own that `migrateJobs` creates on the driver, and prints the results as JSON.
import { nodePostgresDriver } from "kern-jobs/db/node-postgres"
import { postgresJsDriver } from "kern-jobs/db/postgres-js"
import { migrateJobs } from "kern-jobs/migrations"
import { type Driver, RetryError } from "kern-jobs/worker"
import pg from "pg"
import postgres from "postgres"

import { createPgJobs, enqueueIndex } from "./jobs.ts"
import { purgeExpiredSessionsTask } from "./tasks.ts"
import { startConsumerWorker } from "./worker/start.ts"

const url = process.env.DATABASE_URL
if (!url) throw new Error("Set DATABASE_URL to a PostgreSQL database")

/** Resolves with the value of the first call of the callback that `subscribe` gets, or rejects after 10 seconds. */
function waitFor<T>(what: string, subscribe: (resolve: (value: T) => void) => void): Promise<T> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`${what} not seen within 10 s`)), 10_000)
    subscribe(value => {
      clearTimeout(timer)
      resolve(value)
    })
  })
}

/**
 * Runs the worker on a driver until it completed one job and failed another, which waits for its retry, and then in a schema that is not migrated until it reports that.
 */
async function runOn(driver: Driver) {
  const schemaName = `package_${Math.random().toString(36).slice(2, 10)}`
  const pool = new pg.Pool({ connectionString: url, max: 1 })
  try {
    await migrateJobs(driver, { schemaName })
    const jobs = createPgJobs(pool, schemaName)
    await enqueueIndex(jobs, "42", { delay: 0 })
    await jobs.enqueue(purgeExpiredSessionsTask)
    const rows = async () => {
      const result = await pool.query(
        `select task, state, attempts from "${schemaName}".job order by id`,
      )
      return result.rows.map(row => [row.task, row.state, row.attempts])
    }

    let report: (error: unknown) => void = () => {}
    const worker = startConsumerWorker(driver, schemaName, 2, error => report(error))
    const retry = await waitFor<unknown>("the error of the handler", resolve => (report = resolve))
    // The worker reports no completion, so the completed job is awaited in the table.
    const deadline = Date.now() + 10_000
    while ((await rows()).length > 1 && Date.now() < deadline) {
      await new Promise(resolve => setTimeout(resolve, 20))
    }
    await worker.stop()

    const missing = startConsumerWorker(driver, `${schemaName}_missing`, undefined, (error, info) =>
      report(info.source === "schema" ? error : info.source),
    )
    const schemaError = await waitFor<unknown>(
      "the error of the schema",
      resolve => (report = resolve),
    )
    await missing.stop()

    return {
      retryDelay: retry instanceof RetryError ? retry.delay : null,
      rows: await rows(),
      schemaError: schemaError instanceof Error ? schemaError.message : schemaError,
    }
  } finally {
    try {
      await pool.query(`drop schema if exists "${schemaName}" cascade`)
    } finally {
      await Promise.all([pool.end(), driver.close()])
    }
  }
}

console.log(
  JSON.stringify({
    "postgres.js": await runOn(postgresJsDriver(postgres(url, { max: 3 }))),
    "node-postgres": await runOn(
      nodePostgresDriver(new pg.Pool({ connectionString: url, max: 3 })),
    ),
  }),
)
