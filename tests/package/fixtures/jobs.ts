// Jobs of the consumer through node-postgres and postgres.js, and a wrapper of enqueue, exported with inferred types that its declarations name through the exports of kern-jobs; the type tests of `declaration-consumer/` check the emitted types.
import { createJobs, type Jobs } from "kern-jobs"
import type pg from "pg"
import type postgres from "postgres"

import { indexProductTask } from "./tasks.ts"

export function createPgJobs(client: pg.Pool | pg.PoolClient, schemaName: string) {
  return createJobs(client, { schemaName })
}

export function createPostgresJobs(sql: postgres.Sql, schemaName: string) {
  return createJobs(sql, { schemaName })
}

/** Options of `enqueueIndex`, which it passes through to the overrides as they are, absent ones as `undefined`. */
export interface IndexOptions {
  key?: string | null
  queue?: string | null
  maxAttempts?: number
  delay?: number
  maxWait?: number
  resetAttempts?: boolean
}

/** Enqueues the indexing of a product; `maxWait` debounces it. */
export function enqueueIndex(jobs: Jobs, productId: string, options: IndexOptions = {}) {
  const { key, queue, maxAttempts, delay, maxWait, resetAttempts } = options
  const keyMode = maxWait === undefined ? undefined : "debounce"
  return jobs.enqueue(
    indexProductTask,
    { productId },
    { key, queue, maxAttempts, delay, keyMode, maxWait, resetAttempts },
  )
}
