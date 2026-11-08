// Test helpers of the worker runtime: recorded events, a worker of a test schema with short timings, and terminating the backends of a pool.
import { reap } from "../core/lease"
import { query, type StatementTarget } from "../core/statement"
import type { TestSchema } from "../core/testing"

import type { WorkerEvent } from "./events"
import { type RuntimeOptions, startRuntime, type Worker } from "./runtime"

/** Events of a worker in the order of their emission. */
export interface EventLog {
  events: WorkerEvent[]
  onEvent(event: WorkerEvent): void
  /** Returns the first event that matches, waiting for it up to `timeoutMs`. */
  waitFor<T extends WorkerEvent["type"]>(
    type: T,
    pred?: (e: Extract<WorkerEvent, { type: T }>) => boolean,
    timeoutMs?: number,
  ): Promise<Extract<WorkerEvent, { type: T }>>
  /** Returns the events of a type that match. */
  all<T extends WorkerEvent["type"]>(
    type: T,
    pred?: (e: Extract<WorkerEvent, { type: T }>) => boolean,
  ): Extract<WorkerEvent, { type: T }>[]
}

/** Creates an event log to pass as `onEvent` of a worker. */
export function recordEvents(): EventLog {
  const events: WorkerEvent[] = []
  const waiters = new Set<() => void>()
  const all: EventLog["all"] = (type, pred) =>
    events.filter(e => e.type === type && (!pred || pred(e as never))) as never
  return {
    events,
    onEvent(event) {
      events.push(event)
      for (const w of waiters) w()
    },
    all,
    waitFor(type, pred, timeoutMs = 10_000) {
      return new Promise((resolve, reject) => {
        const check = () => {
          const found = all(type, pred)[0]
          if (!found) return false
          waiters.delete(check)
          clearTimeout(timer)
          resolve(found)
          return true
        }
        const timer = setTimeout(() => {
          waiters.delete(check)
          const recent = events.slice(-5).map(e => e.type)
          reject(
            new Error(
              `event ${type} not seen within ${timeoutMs} ms; recent: ${recent.join(", ")}`,
            ),
          )
        }, timeoutMs)
        if (!check()) waiters.add(check)
      })
    },
  }
}

/** Timings of test workers in seconds: a short lease, frequent heartbeats, and fast polling. */
export const testTimings = {
  lease: 1,
  heartbeatInterval: 0.1,
  abortMargin: 0.3,
  pollInterval: 0.02,
} as const

/**
 * Starts a worker on the pool of a test schema with test timings and without a reaper.
 * Call `stopTestWorkers` before `dispose` of the schema, whose pools wait for the connections that workers hold.
 */
export function startTestWorker(
  schema: TestSchema,
  options: Partial<RuntimeOptions> & Pick<RuntimeOptions, "tasks">,
): Worker {
  const worker = startRuntime({
    driver: schema.driver,
    table: schema.table,
    id: `test-${++started}`,
    concurrency: 1,
    reaper: false,
    ...testTimings,
    ...options,
  })
  workers.add(worker)
  return worker
}

const workers = new Set<Worker>()
let started = 0

/** Stops the workers that `startTestWorker` started and that are still running. */
export async function stopTestWorkers(): Promise<void> {
  // A worker that cannot stop fails the teardown of its own test only.
  const stopping = [...workers]
  workers.clear()
  // A stop rejects only at a timeout that the test passed to it, which the test has checked.
  await Promise.all(stopping.map(w => w.stop({ abortAfter: 0 }).catch(() => {})))
}

/** Terminates the backends of the connections with an `application_name`; returns how many it terminated. */
export async function terminateBackends(
  observer: StatementTarget,
  applicationName: string,
): Promise<number> {
  const rows = await query(
    observer,
    `select pg_terminate_backend(pid) from pg_stat_activity
     where application_name = $1::text and pid <> pg_backend_pid()`,
    [applicationName],
  )
  return rows.length
}

/** Moves the lease of a job into the past and reaps it in one transaction, while its handler may still run. */
export async function reapNow(schema: TestSchema, id: string): Promise<void> {
  const conn = await schema.connect()
  await query(conn, "begin")
  await query(
    conn,
    `update ${schema.table.name} set lease_until = now() - interval '1 second' where id = $1::text::bigint`,
    [id],
  )
  await reap(conn, schema.table)
  await query(conn, "commit")
  await conn.close()
}
