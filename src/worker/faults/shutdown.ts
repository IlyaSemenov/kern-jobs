// Process for the tests of the helpers of signals: a fake worker whose stop resolves, rejects, throws, hangs, or waits until it is forced, and the close or cleanup of the application, as `SHUTDOWN_CONFIG` says; it prints each phase on its own line, so that the test sends the next signal after it.

import { existsSync } from "node:fs"

import type { StopOptions, Worker } from "../runtime"
import { stopOnSignals, waitForSignalAndStop } from "../shutdown"

/** How the stop of the worker and the close of the application end, and which helper the process uses. */
export interface ShutdownConfig {
  helper: "stopOnSignals" | "waitForSignalAndStop"
  /** How the stop ends; `throw` throws before it returns a promise. */
  stop: Outcome | "until-forced" | "throw"
  /** `close` of `stopOnSignals`, or the cleanup that the application runs after `waitForSignalAndStop`. */
  close: Outcome
  /** File whose creation ends an operation of the outcome `gate`. */
  gateFile?: string
  /** Timeout of `stopOnSignals` in seconds. */
  timeout?: number
  /** Whether the process sends itself SIGTERM once it listens. */
  selfSignal?: boolean
}

/** How a simulated operation settles: it resolves, resolves once the test creates `gateFile`, rejects, or never settles. */
export type Outcome = "resolve" | "gate" | "reject" | "hang"

const cfg: ShutdownConfig = JSON.parse(process.env.SHUTDOWN_CONFIG!)

/** Returns a promise that ends as `outcome` says. */
function settleAs(outcome: Outcome, name: string): Promise<void> {
  if (outcome === "resolve") return Promise.resolve()
  if (outcome === "gate") {
    return new Promise(resolve => {
      const timer = setInterval(() => {
        if (!existsSync(cfg.gateFile!)) return
        clearInterval(timer)
        resolve()
      }, 20)
    })
  }
  if (outcome === "reject") return Promise.reject(new Error(`${name} failed`))
  return new Promise(() => {})
}

const forced = Promise.withResolvers<void>()
let stopped: Promise<void> | undefined
const worker: Pick<Worker, "stop"> = {
  stop(options?: StopOptions) {
    if (options?.force) {
      console.log("force")
      forced.reject(new Error("The worker stop was forced"))
      return stopped!
    }
    console.log("stop")
    if (cfg.stop === "throw") throw new Error("stop failed")
    stopped = cfg.stop === "until-forced" ? forced.promise : settleAs(cfg.stop, "stop")
    return stopped
  },
}

/** Logs and runs the close or cleanup of the application. */
function close() {
  console.log("close")
  return settleAs(cfg.close, "close")
}

if (cfg.helper === "stopOnSignals") {
  stopOnSignals(worker, { timeout: cfg.timeout, close })
} else {
  void (async () => {
    let code = 0
    try {
      await waitForSignalAndStop(worker)
    } catch (error) {
      console.error(error)
      code = 1
    }
    await close()
    console.log("closed")
    process.exit(code)
  })()
}
// Keeps the process alive until a helper or the application exits it.
setInterval(() => {}, 1000)
console.log("listening")
if (cfg.selfSignal) process.kill(process.pid, "SIGTERM")
