// Process for the tests of `stopOnSignals`: a fake worker whose stop and the close of the application resolve, reject, or hang as `SHUTDOWN_CONFIG` says, and a SIGTERM that the process sends itself.
import type { Worker } from "../runtime"
import { stopOnSignals } from "../shutdown"

/** How the stop of the worker and the close of the application end. */
export interface ShutdownConfig {
  stop: Outcome
  close: Outcome
  /** Timeout of `stopOnSignals` in seconds. */
  timeout: number
}

/** How a simulated operation settles: it resolves, rejects, or never settles. */
export type Outcome = "resolve" | "reject" | "hang"

const cfg: ShutdownConfig = JSON.parse(process.env.SHUTDOWN_CONFIG!)

/** Returns a promise that ends as `outcome` says. */
function settleAs(outcome: Outcome, name: string): Promise<void> {
  if (outcome === "resolve") return Promise.resolve()
  if (outcome === "reject") return Promise.reject(new Error(`${name} failed`))
  return new Promise(() => {})
}

const worker: Pick<Worker, "stop"> = {
  stop: () => {
    console.log("stop")
    return settleAs(cfg.stop, "stop")
  },
}
stopOnSignals(worker, {
  timeout: cfg.timeout,
  close: () => {
    console.log("close")
    return settleAs(cfg.close, "close")
  },
})
// Keeps the process alive until the helper exits it.
setInterval(() => {}, 1000)
process.kill(process.pid, "SIGTERM")
