// Worker process for fault-injection tests: runs the worker runtime with handlers that the configuration describes and logs its events synchronously as JSON lines, so the log survives SIGKILL.
// Configured by the FAULT_CONFIG environment variable.
import { appendFileSync, existsSync } from "node:fs"

import { type Driver, TransactionConnectionError } from "kern-db"
import { getBackendPid } from "kern-db/testing"

import { createJobTable } from "../../core/schema"
import { createTestDriver } from "../../core/testing"
import type { WorkerEvent } from "../events"
import { startRuntime, type WorkerTask } from "../runtime"
import { stopOnSignals } from "../shutdown"

/** Worker process configuration. */
export interface WorkerConfig {
  id: string
  /** A worker runs handlers without a reaper; a reaper runs only the reaper. */
  role: "worker" | "reaper"
  schemaName: string
  mainUrl: string
  heartbeatUrl: string
  log: string
  tasks: string[]
  slots: number
  /** Lease in seconds. */
  lease: number
  heartbeatMs: number
  /** The handler is aborted when its lease cannot be confirmed this long before it ends. */
  marginMs: number
  handlerMs: number
  honorAbort: boolean
  /** Every handler fails with a retryable error instead of completing. */
  failAll?: boolean
  /** Backoff of a retry in seconds. */
  backoff?: number
  /** Handlers end only once this file exists, after their `handlerMs`. */
  gateFile?: string
  /** Handlers that honor the abort throw on it only once this file exists. */
  abortGateFile?: string
  /** Blocks forever at this point so the test can SIGKILL the process there. */
  pauseAt?: "after-claim" | "mid-fail"
  /** Size of the pool of claims and settlements; with 1, the ready event reports the pid of its backend. */
  poolSize?: number
  /** Breakpoints enabled on the pool. */
  hooks?: string[]
  /** Breakpoints enabled on the heartbeat connection. */
  heartbeatHooks?: string[]
  /** `client_connection_check_interval` of the pool, ms. */
  connectionCheckMs?: number
  pollMs?: number
  /** Stops the worker with these options of `stopOnSignals` on SIGTERM and SIGINT, closing the pools, and exits. */
  shutdown?: { abortAfter: number; timeout: number }
}

const cfg: WorkerConfig = JSON.parse(process.env.FAULT_CONFIG!)

const log = (ev: string, data: Record<string, unknown> = {}) =>
  appendFileSync(cfg.log, JSON.stringify({ t: Date.now(), w: cfg.id, ev, ...data }) + "\n")

const hang = () => new Promise<never>(() => {})
const sleep = (ms: number) => new Promise(r => setTimeout(r, ms))

/**
 * Makes a reserved connection hang after the competitor check of a fail transaction, which then holds the rows of the claim and of the pending job of its key until the process dies.
 */
function pauseMidFail(driver: Driver): Driver {
  return {
    ...driver,
    async acquire() {
      const connection = await driver.acquire()
      return {
        ...connection,
        async execute(statement) {
          const result = await connection.execute(statement)
          if (statement.text.includes(`key_mode from ${table.name} where state = 'pending'`)) {
            log("paused", { at: "mid-fail" })
            await hang()
          }
          return result
        },
      }
    },
  }
}

const table = createJobTable(cfg.schemaName)
const pool = createTestDriver(cfg.mainUrl, cfg.schemaName, {
  max: cfg.poolSize ?? 4,
  hooks: cfg.hooks,
  connectionCheckMs: cfg.connectionCheckMs,
})
const heartbeatSource = createTestDriver(cfg.heartbeatUrl, cfg.schemaName, {
  max: 1,
  hooks: cfg.heartbeatHooks,
})
log("ready", { mainPid: cfg.poolSize === 1 ? await getBackendPid(pool) : undefined })

const task: WorkerTask = {
  async handler(_job, { signal }) {
    if (cfg.pauseAt === "after-claim") {
      log("paused", { at: "after-claim" })
      await hang()
    }
    const started = Date.now()
    while (Date.now() - started < cfg.handlerMs) {
      const gated = cfg.abortGateFile && !existsSync(cfg.abortGateFile)
      if (signal.aborted && cfg.honorAbort && !gated) throw signal.reason
      await sleep(10)
    }
    while (cfg.gateFile && !existsSync(cfg.gateFile)) await sleep(10)
    if (cfg.failAll) throw new Error("injected")
  },
  backoff: () => cfg.backoff ?? 0,
}

const message = (error: unknown) => (error as Error)?.message ?? String(error)

/** Logs an event of the worker as a JSON line. */
function onEvent(e: WorkerEvent) {
  switch (e.type) {
    case "start":
      return log("start", {
        id: e.claim.id,
        claimId: e.claim.claimId,
        queue: e.claim.job.queue,
        attempts: e.claim.job.attempts,
        lag: e.claim.lag,
      })
    case "end":
      return log("end", {
        id: e.claim.id,
        claimId: e.claim.claimId,
        abort: e.aborted,
        failed: e.failed,
      })
    case "settled":
      return log("settled", { id: e.claim.id, claimId: e.claim.claimId, outcome: e.outcome })
    case "settle-error":
      return log("settle-error", {
        id: e.claim.id,
        message: message(e.error),
        phase: e.error instanceof TransactionConnectionError ? e.error.phase : "statement",
      })
    case "abort":
      return log("abort", { id: e.claim.id, claimId: e.claim.claimId, reason: e.reason })
    case "heartbeat":
      return log("heartbeat", { claims: e.claims })
    case "reap":
      return log("reap", { ids: e.jobs.map(j => j.id) })
    case "heartbeat-recovered":
      return log(e.type)
    case "heartbeat-error":
    case "claim-error":
    case "reap-error":
    case "error":
      return log(e.type, { message: message(e.error) })
  }
}

const worker = startRuntime({
  id: cfg.id,
  driver: cfg.pauseAt === "mid-fail" ? pauseMidFail(pool) : pool,
  table,
  heartbeatSource,
  tasks: Object.fromEntries((cfg.role === "worker" ? cfg.tasks : []).map(t => [t, task])),
  concurrency: cfg.role === "worker" ? cfg.slots : 0,
  lease: cfg.lease,
  heartbeatInterval: cfg.heartbeatMs / 1000,
  abortMargin: cfg.marginMs / 1000,
  pollInterval: (cfg.pollMs ?? 50) / 1000,
  reaper: cfg.role === "reaper" ? { interval: (cfg.pollMs ?? 200) / 1000, limit: 1000 } : false,
  onEvent,
})

if (cfg.shutdown) {
  for (const signal of ["SIGTERM", "SIGINT"] as const)
    process.on(signal, () => log("signal", { signal }))
  stopOnSignals(worker, {
    ...cfg.shutdown,
    close: () => {
      log("close")
      return Promise.all([pool.close(), heartbeatSource.close()])
    },
  })
}
