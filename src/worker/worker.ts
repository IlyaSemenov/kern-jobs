// Worker of an application: checks its options, fills their defaults, and starts the runtime on a driver of kern-db with the tasks of a manifest, which checks its schema before it takes jobs, reporting the errors of the runtime to `onError`.
import { hostname } from "node:os"

import type { Driver } from "kern-db"

import { checkCount } from "../core/db"
import { checkDriver } from "../core/driver"
import { createJobTable, DEFAULT_SCHEMA } from "../core/schema"
import { checkJobSchema } from "../migration/migrate"
import type { Manifest } from "../task/manifest"
import { isLease, type JobInfo, leaseRange } from "../task/task"

import type { AbortReason, WorkerEvent } from "./events"
import { intervalRange, isInterval, isTimerDelay, timerDelayRange } from "./loop"
import { startRuntime, type StopOptions, type Worker } from "./runtime"
import { toJobInfo, toWorkerTasks } from "./tasks"

/** Options of the reaper of a worker. */
export interface WorkerReaperOptions {
  /** Seconds between the searches for jobs whose lease expired; 5 by default. */
  interval?: number | undefined
  /** Jobs that one search returns to pending; a search that returns as many starts the next one at once; 100 by default. */
  limit?: number | undefined
}

/**
 * Details of an error of a worker, which `onError` gets beside the error, by `source`:
 * - `handler`: the handler of a job threw, and the job retries or fails; `aborted` tells why its signal was aborted before that, or is `null`;
 * - `backoff`: the backoff of the task threw or returned no valid delay, and the job retries with the default backoff;
 * - `settlement`: a statement that records the outcome of a job failed; the worker retries it, and a job whose outcome it cannot record returns to pending once its lease expires, or fails on its last attempt;
 * - `schema`: the schema of kern-jobs is not up to date, such as before `migrateJobs`, or checking it failed; the worker takes no jobs and checks again after the poll interval;
 * - `claim`: taking jobs failed, which the worker tries again after the poll interval, or a job was taken too late to keep its lease, and the worker failed it without running its handler;
 * - `heartbeat`: extending the leases of the running jobs failed, which the worker tries again;
 * - `reaper`: returning the jobs of expired leases failed, which the worker tries again;
 * - `release`: the driver failed to release a connection;
 * - `worker`: the worker itself failed unexpectedly and goes on.
 */
export type WorkerErrorInfo =
  | { source: "handler"; task: string; job: JobInfo; aborted: AbortReason | null }
  | { source: "backoff" | "settlement"; task: string; job: JobInfo }
  | { source: "schema" | "claim" | "heartbeat" | "reaper" | "release" | "worker" }

/** Options of `startWorker`. */
export interface WorkerOptions {
  /** Implementations of the tasks whose jobs the worker runs. */
  manifest: Manifest
  /** PostgreSQL schema of the tables of kern-jobs, which `migrateJobs` created; `kern_jobs` by default. */
  schemaName?: string | undefined
  /** Jobs that the worker runs at once; 1 by default. */
  concurrency?: number | undefined
  /** Name of the worker, which the jobs that it runs store; made of the host and the process by default. */
  id?: string | undefined
  /** Lease in seconds of the jobs of tasks without a `lease` of their own; 60 by default. */
  lease?: number | undefined
  /** Seconds between the checks for jobs to run while the worker has free slots; 1 by default. */
  pollInterval?: number | undefined
  /**
   * Reaper of the worker, which returns the jobs of expired leases, such as of a worker that died, to pending; `false` to run none, such as when another process runs one.
   */
  reaper?: false | WorkerReaperOptions | undefined
  /**
   * Receives every error of the worker and of its handlers, synchronously, with what failed; an error that it throws is ignored.
   * By default, the worker writes them with `console.error`.
   */
  onError?: ((error: unknown, info: WorkerErrorInfo) => void) | undefined
}

/** Checks an option that a loop of the worker takes as its interval. */
function checkInterval(name: string, value: unknown): asserts value is number {
  if (!isInterval(value)) {
    throw new RangeError(`${name} must be ${intervalRange}, got ${String(value)}`)
  }
}

/** Checks the options of `stop`. */
function checkStopOptions(options: unknown): asserts options is StopOptions | undefined {
  if (options === undefined) return
  if (typeof options !== "object" || options === null) {
    throw new TypeError("The options of stop must be an object")
  }
  const { abortAfter, timeout } = options as Record<string, unknown>
  for (const [name, value] of [
    ["abortAfter", abortAfter],
    ["timeout", timeout],
  ] as const) {
    if (value !== undefined && !isTimerDelay(value))
      throw new RangeError(`${name} must be ${timerDelayRange}, got ${String(value)}`)
  }
}

/** Returns the error and what failed of an event that reports an error, or `null` for any other event. */
function toErrorReport(event: WorkerEvent): [error: unknown, info: WorkerErrorInfo] | null {
  switch (event.type) {
    case "end": {
      // A handler interrupted by stop did not fail: its job returns to pending without spending its attempt.
      if (!event.failed || event.interrupted) return null
      const { job } = event.claim
      return [
        event.error,
        { source: "handler", task: job.task, job: toJobInfo(job), aborted: event.aborted },
      ]
    }
    case "backoff-error":
    case "settle-error": {
      const { job } = event.claim
      const source = event.type === "backoff-error" ? "backoff" : "settlement"
      return [event.error, { source, task: job.task, job: toJobInfo(job) }]
    }
    case "schema-error":
      return [event.error, { source: "schema" }]
    case "claim-error":
      return [event.error, { source: "claim" }]
    case "late-claim": {
      const { job } = event.claim
      const error = new Error(
        `Job ${job.id} of task ${job.task} was taken after the deadline of its lease, so its handler did not run, and the attempt counts`,
      )
      return [error, { source: "claim" }]
    }
    case "heartbeat-error":
      return [event.error, { source: "heartbeat" }]
    case "reap-error":
      return [event.error, { source: "reaper" }]
    case "release-error":
      return [event.error, { source: "release" }]
    case "error":
      return [event.error, { source: "worker" }]
    default:
      return null
  }
}

/** Writes an error of a worker with `console.error`, the `onError` of a worker without one. */
function logError(error: unknown, info: WorkerErrorInfo): void {
  const job = "job" in info ? `, task ${info.task}, job ${info.job.id}` : ""
  console.error(`kern-jobs: ${info.source} error${job}:`, error)
}

/**
 * Starts a worker that runs the jobs of the tasks of a manifest until `stop`.
 * It runs on `driver`, a driver of kern-db over the database client of the worker, such as `postgresJsDriver(sql)` of `kern-jobs/db/postgres-js`, outside the transactions of the application; the worker keeps one connection of its pool for the heartbeats, so the pool needs at least two connections, and with one it never becomes ready.
 * It throws `TypeError` or `RangeError` for a wrong driver or option before it starts anything; a failure of the database later, such as a schema that `migrateJobs` has not created, goes to `onError`, and the worker keeps trying: await `ready` or check `state` to know whether it takes jobs.
 */
export function startWorker(driver: Driver, options: WorkerOptions): Worker {
  const {
    concurrency = 1,
    id = `${hostname()}:${process.pid}:${Math.random().toString(36).slice(2, 6)}`,
    lease = 60,
    pollInterval = 1,
    reaper = {},
    onError = logError,
    schemaName = DEFAULT_SCHEMA,
  } = options
  checkDriver(driver)
  const tasks = toWorkerTasks(options.manifest)
  const table = createJobTable(schemaName)
  checkCount("concurrency", concurrency)
  // PostgreSQL rejects U+0000 in a text value, so every claim of such a worker would fail.
  if (typeof id !== "string" || id === "" || id.includes("\0")) {
    throw new TypeError("id must be a string that is not empty and has no U+0000")
  }
  if (!isLease(lease)) throw new RangeError(`lease must be ${leaseRange}, got ${String(lease)}`)
  checkInterval("pollInterval", pollInterval)
  if (reaper !== false && (typeof reaper !== "object" || reaper === null)) {
    throw new TypeError("reaper must be false or an options object")
  }
  const { interval = 5, limit = 100 } = reaper || {}
  checkInterval("reaper.interval", interval)
  checkCount("reaper.limit", limit)
  if (typeof onError !== "function") throw new TypeError("onError must be a function")

  const runtime = startRuntime({
    driver,
    table,
    tasks,
    concurrency,
    id,
    lease,
    pollInterval,
    reaper: reaper && { interval, limit },
    checkSchema: database => checkJobSchema(database, schemaName),
    onEvent(event) {
      const report = toErrorReport(event)
      if (report) onError(...report)
    },
  })
  let stopped: Promise<void> | undefined
  return {
    id: runtime.id,
    ready: runtime.ready,
    get state() {
      return runtime.state
    },
    stop(stopOptions) {
      // A later call joins the stop in progress, whatever its options.
      if (!stopped) {
        checkStopOptions(stopOptions)
        stopped = runtime.stop(stopOptions)
      }
      return stopped
    },
  }
}
