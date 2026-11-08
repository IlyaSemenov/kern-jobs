// Migrates a schema of its own, enqueues one job in it through node-postgres and postgres.js, checks the manifest and runs a handler of the worker, and prints the results as JSON.
import { nodePostgresDriver } from "kern-jobs/db/node-postgres"
import { migrateJobs } from "kern-jobs/migrations"
import { defineManifest, RetryError } from "kern-jobs/worker"
import pg from "pg"
import postgres from "postgres"

import { createPgJobs, createPostgresJobs, enqueueIndex } from "./jobs.ts"
import { manifest, unimplementedTasks } from "./worker/manifest.ts"
import indexProduct from "./worker/tasks/index-product.ts"

const url = process.env.DATABASE_URL
if (!url) throw new Error("Set DATABASE_URL to a PostgreSQL database")

/** Returns the error that a function throws, or `undefined`. */
async function catchError(fn: () => unknown): Promise<unknown> {
  try {
    await fn()
    return undefined
  } catch (error) {
    return error
  }
}

const schemaName = `package_${Math.random().toString(36).slice(2, 10)}`
const pool = new pg.Pool({ connectionString: url, max: 1 })
const sql = postgres(url, { max: 1 })
try {
  await migrateJobs(nodePostgresDriver(pool), { schemaName })
  const inserted = await enqueueIndex(createPgJobs(pool, schemaName), "42")
  const merged = await enqueueIndex(createPostgresJobs(sql, schemaName), "42", { delay: 0 })
  const duplicate = await catchError(() =>
    defineManifest({ tasks: [...manifest.tasks, indexProduct] }),
  )
  const retry = await catchError(() =>
    indexProduct.handler(
      { productId: "42" },
      {
        signal: new AbortController().signal,
        job: {
          id: inserted.id,
          scope: null,
          attempt: 1,
          maxAttempts: 3,
          key: "product:42",
          queue: null,
        },
      },
    ),
  )
  console.log(
    JSON.stringify({
      created: [inserted.created, merged.created],
      sameJob: merged.id === inserted.id,
      manifest: manifest.tasks.map(implementation => implementation.task.name),
      unimplementedTasks,
      duplicateRejected: duplicate instanceof Error,
      retryDelay: retry instanceof RetryError ? retry.delay : null,
    }),
  )
} finally {
  try {
    await pool.query(`drop schema if exists "${schemaName}" cascade`)
  } finally {
    await Promise.all([pool.end(), sql.end()])
  }
}
