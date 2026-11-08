// Periodic rounds of a worker that run one at a time: claiming, heartbeats, and the reaper.

/** Longest delay of `setTimeout` in milliseconds; a timer of a longer delay fires at once. */
export const maxTimerDelay = 2 ** 31 - 1

/** Longest interval of a loop in whole seconds, about 24.8 days, which one timer waits. */
export const maxInterval = Math.floor(maxTimerDelay / 1000)

/** Range of an interval of a loop, for error messages. */
export const intervalRange = `a number of seconds above 0 up to ${maxInterval}`

/** Whether a value is an interval of a loop: seconds that one timer waits, and more than 0, so that rounds pause. */
export function isInterval(value: unknown): value is number {
  return typeof value === "number" && value > 0 && value <= maxInterval
}

/** Range of a delay of one timer in seconds, for error messages. */
export const timerDelayRange = `a number of seconds from 0 up to ${maxInterval}`

/** Whether a value is a delay in seconds that one timer waits, such as an option of a stop. */
export function isTimerDelay(value: unknown): value is number {
  return typeof value === "number" && value >= 0 && value <= maxInterval
}

/** Rounds of a periodic task, started by `startLoop`. */
export interface Loop {
  /** Starts the next round at once, after the running one if any. */
  wake(): void
  /** Stops starting rounds and waits for the running one. */
  stop(): Promise<void>
}

/** Options of a loop. */
export interface LoopOptions {
  /** Seconds from the end of a round to the start of the next one, at most `maxInterval`. */
  interval: number
  /** Receives the error of a round that throws; the loop goes on. */
  onError(error: unknown): void
}

/**
 * Runs `round` repeatedly, one round at a time, starting at once.
 * The next round starts `interval` seconds after the previous one ends, after the number of seconds that the round returns, or at once after `wake`, so rounds never overlap, however long one takes.
 */
export function startLoop(round: () => Promise<number | void>, o: LoopOptions): Loop {
  let stopped = false
  let woken = false
  let interrupt: (() => void) | undefined
  const done = (async () => {
    while (!stopped) {
      woken = false
      let delay: number | void
      try {
        delay = await round()
      } catch (error) {
        o.onError(error)
      }
      // A wake during the round starts the next one without a pause.
      if (stopped || woken) continue
      await new Promise<void>(resolve => {
        const timer = setTimeout(resolve, (delay ?? o.interval) * 1000)
        interrupt = () => {
          clearTimeout(timer)
          resolve()
        }
      })
      interrupt = undefined
    }
  })()
  return {
    wake() {
      woken = true
      interrupt?.()
    },
    stop() {
      stopped = true
      interrupt?.()
      return done
    },
  }
}

/** Resolves after `seconds`. */
export const sleep = (seconds: number) =>
  new Promise<void>(resolve => setTimeout(resolve, seconds * 1000))

/** Monotonic time in milliseconds, which wall-clock adjustments do not move. */
export const monotonicNow = () => performance.now()
