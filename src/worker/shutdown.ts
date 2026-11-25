// Stop of a worker at a termination signal of the process: `waitForSignalAndStop` returns the outcome of the stop to the application, and `stopOnSignals` then calls the `close` of the application and exits the process within a timeout.

import { isTimerDelay, monotonicNow, timerDelayRange } from "./loop"
import type { StopOptions, Worker } from "./runtime"

/** Options of `waitForSignalAndStop`. */
export interface WaitForSignalAndStopOptions {
  /** Seconds from the signal during which running handlers may end before their signals are aborted, as `abortAfter` of `stop`; 5 by default. */
  abortAfter?: number | undefined
  /** Seconds from the signal after which the stop stops waiting and rejects, as `timeout` of `stop`; without it, the stop waits as long as the handlers run. */
  timeout?: number | undefined
}

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
 * Milliseconds from the first signal during which another signal joins the stop rather than forces it.
 * One Ctrl+C can reach the process twice, from the terminal and from a parent runner that forwards it, and an orchestrator and a wrapper may each send a signal.
 */
export const signalWindow = 2000

const stopSignals = ["SIGTERM", "SIGINT"] as const

/** Calls `stop` of a worker and returns its promise, which rejects with what `stop` throws. */
function callStop(worker: Pick<Worker, "stop">, options: StopOptions): Promise<void> {
  try {
    return worker.stop(options)
  } catch (error) {
    return Promise.reject(error)
  }
}

/**
 * Listens for SIGTERM and SIGINT and returns the outcome of the stop that the first of them begins with `stopOptions`, after calling `onFirstSignal`.
 * A signal within `signalWindow` of the first, by the clock `now`, is ignored, and a later one while the stop runs forces it and removes the listeners; the caller gets the error of a force that throws or rejects.
 * Once the stop has settled and the window has passed, the listeners are removed, so that the next signal gets its default action unless the application listens for it.
 */
export function listenForStopSignals(
  worker: Pick<Worker, "stop">,
  stopOptions: StopOptions,
  o: { onFirstSignal?: () => void; now?: () => number } = {},
): Promise<void> {
  const now = o.now ?? monotonicNow
  const outcome = Promise.withResolvers<void>()
  let firstAt: number | undefined
  let settled = false
  let listening = true
  const isWithinWindow = () => now() - firstAt! < signalWindow
  const removeListeners = () => {
    listening = false
    for (const signal of stopSignals) process.off(signal, onSignal)
  }
  const removeListenersOnceDone = () => {
    if (listening && settled && !isWithinWindow()) removeListeners()
  }
  /** At the end of the window, removes the listeners if the stop has settled; checks again when its timer runs before the clock reaches the end. */
  const watchWindow = (delay: number) => {
    // The timer keeps no process alive that has nothing else to do.
    setTimeout(() => {
      if (!listening) return
      if (isWithinWindow()) watchWindow(signalWindow - (now() - firstAt!))
      else removeListenersOnceDone()
    }, delay).unref()
  }
  const onSignal = () => {
    if (firstAt === undefined) {
      firstAt = now()
      watchWindow(signalWindow)
      o.onFirstSignal?.()
      callStop(worker, stopOptions).then(
        () => {
          settled = true
          removeListenersOnceDone()
          outcome.resolve()
        },
        (error: unknown) => {
          settled = true
          removeListenersOnceDone()
          outcome.reject(error)
        },
      )
      return
    }
    if (isWithinWindow()) return
    removeListeners()
    // The listeners outlived the window only because its timer has not run yet, such as while synchronous cleanup blocks the event loop; the next signal gets its default action.
    if (settled) return
    callStop(worker, { force: true }).catch((error: unknown) => outcome.reject(error))
  }
  for (const signal of stopSignals) process.on(signal, onSignal)
  return outcome.promise
}

/** Checks an option of a helper of signals that is a delay in seconds. */
function toDelay(name: string, value: unknown): number | undefined {
  if (value !== undefined && !isTimerDelay(value))
    throw new RangeError(`${name} must be ${timerDelayRange}, got ${String(value)}`)
  return value
}

/** Checks the options of a helper of signals and returns the values it checked, so that a getter read again cannot change them. */
function toSignalOptions(name: string, options: unknown) {
  if (typeof options !== "object" || options === null) {
    throw new TypeError(`The options of ${name} must be an object`)
  }
  const { abortAfter, timeout } = options as Record<string, unknown>
  return { abortAfter: toDelay("abortAfter", abortAfter), timeout: toDelay("timeout", timeout) }
}

/**
 * Stops a worker at the first SIGTERM or SIGINT of the process and returns control to the application once the stop has ended: it resolves once the worker has stopped, and rejects with the error of `stop`, such as at `timeout`.
 * It neither closes a database client nor exits the process: run the cleanup of the application after it, also when it rejects, then exit the process.
 * Another of the two signals within 2 seconds of the first joins the stop, and a later one while the stop runs forces it, as `stop` with `force`.
 * It stops listening once a signal has forced the stop, or once the stop has ended and 2 seconds have passed since the first signal; after that, SIGTERM and SIGINT have their usual effect again.
 * It throws `TypeError` or `RangeError` for a wrong option before it listens for the signals.
 */
export function waitForSignalAndStop(
  worker: Pick<Worker, "stop">,
  options: WaitForSignalAndStopOptions = {},
): Promise<void> {
  const { abortAfter, timeout } = toSignalOptions("waitForSignalAndStop", options)
  return listenForStopSignals(worker, { abortAfter, timeout })
}

/**
 * Stops a worker at the first SIGTERM or SIGINT of the process, then calls `close`, and exits the process: with 0 once both succeeded, and with 1, after writing the error with `console.error`, when either failed or `timeout` passed first.
 * Another of the two signals within 2 seconds of the first joins the stop, and a later one while the stop runs forces it, as `stop` with `force`, so that the process calls `close` and exits with 1.
 * It stops listening once a signal has forced the stop, or once the stop has ended and 2 seconds have passed since the first signal; after that, SIGTERM and SIGINT have their usual effect again.
 * It throws `TypeError` or `RangeError` for a wrong option before it listens for the signals.
 */
export function stopOnSignals(
  worker: Pick<Worker, "stop">,
  options: StopOnSignalsOptions = {},
): void {
  const { abortAfter, timeout = 25 } = toSignalOptions("stopOnSignals", options)
  const { close } = options as Record<string, unknown>
  if (close !== undefined && typeof close !== "function") {
    throw new TypeError("close must be a function")
  }
  let deadline: ReturnType<typeof setTimeout> | undefined
  const stopped = listenForStopSignals(
    worker,
    { abortAfter },
    {
      onFirstSignal() {
        // The worker cancels no query and cannot end a handler that ignores its signal, so the exit bounds the stop.
        deadline = setTimeout(() => {
          console.error(`kern-jobs: shutdown did not finish within ${timeout} s; exiting`)
          process.exit(1)
        }, timeout * 1000)
      },
    },
  )
  void (async () => {
    let code = 0
    try {
      await stopped
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
