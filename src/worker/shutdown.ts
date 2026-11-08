// Stop of a worker process at a termination signal: the worker stops, the application closes its database client, and the process exits within a timeout.
import { isTimerDelay, timerDelayRange } from "./loop"
import type { Worker } from "./runtime"

/** Options of `stopOnSignals`. */
export interface StopOnSignalsOptions {
  /** Seconds from the signal during which running handlers may end before their signals are aborted, as `abortAfter` of `stop`; 5 by default. */
  abortAfter?: number | undefined
  /** Seconds from the signal within which the process exits, also when the handlers or `close` have not ended; 25 by default. */
  timeout?: number | undefined
  /** Closes the database client once the worker has stopped, such as `() => sql.end()`. */
  close?: (() => unknown) | undefined
}

/**
 * Stops a worker at the first SIGTERM or SIGINT of the process, then calls `close`, and exits the process: with 0 once both succeeded, and with 1, after writing the error with `console.error`, when either failed or `timeout` passed first.
 * Another of the two signals joins the stop in progress, and the same signal again ends the process at once, as without a handler, such as a second Ctrl+C.
 * It throws `TypeError` or `RangeError` for a wrong option before it listens for the signals.
 */
export function stopOnSignals(
  worker: Pick<Worker, "stop">,
  options: StopOnSignalsOptions = {},
): void {
  if (typeof options !== "object" || options === null) {
    throw new TypeError("The options of stopOnSignals must be an object")
  }
  const { abortAfter, timeout = 25, close } = options
  for (const [name, value] of [
    ["abortAfter", abortAfter],
    ["timeout", timeout],
  ] as const) {
    if (value !== undefined && !isTimerDelay(value))
      throw new RangeError(`${name} must be ${timerDelayRange}, got ${String(value)}`)
  }
  if (close !== undefined && typeof close !== "function") {
    throw new TypeError("close must be a function")
  }
  let stopping = false
  const onSignal = () => {
    if (stopping) return
    stopping = true
    // The worker cancels no query and cannot end a handler that ignores its signal, so the exit bounds the stop.
    const deadline = setTimeout(() => {
      console.error(`kern-jobs: shutdown did not finish within ${timeout} s; exiting`)
      process.exit(1)
    }, timeout * 1000)
    void (async () => {
      let code = 0
      try {
        await worker.stop({ abortAfter })
      } catch (error) {
        console.error("kern-jobs: stop error:", error)
        code = 1
      }
      try {
        await close?.()
      } catch (error) {
        console.error("kern-jobs: close error:", error)
        code = 1
      }
      clearTimeout(deadline)
      process.exit(code)
    })()
  }
  process.once("SIGTERM", onSignal)
  process.once("SIGINT", onSignal)
}
