// Backoff of a task: the delay of the retry after a failed attempt, the factories of common policies, and the default.
import { delayRange, isDelay } from "./delay"

/** Returns the delay in seconds of the retry after the given attempt, counted from 1, failed. */
export type Backoff = (attempt: number) => number

/** Options of `exponentialBackoff`. */
export interface ExponentialBackoffOptions {
  /** Delay in seconds after the first attempt; 2 by default. */
  base?: number | undefined
  /** Ratio of each delay to the previous one; 2 by default. */
  factor?: number | undefined
  /** Longest delay in seconds; an hour by default. */
  max?: number | undefined
  /**
   * Share from 0 to 1 by which each delay is shortened at random, so that jobs that failed together do not retry together; 0 by default.
   */
  jitter?: number | undefined
}

/** Returns a backoff whose delay grows by `factor` from `base` after each attempt, up to `max`. */
export function exponentialBackoff(options: ExponentialBackoffOptions = {}): Backoff {
  const { base = 2, factor = 2, max = 3600, jitter = 0 } = options
  // A base of 0 would turn the growth of a late attempt, Infinity, into NaN.
  if (!(isDelay(base) && base > 0)) {
    throw new RangeError(`base must be ${delayRange}, other than 0, got ${String(base)}`)
  }
  checkDelay("max", max)
  if (!(typeof factor === "number" && Number.isFinite(factor) && factor >= 1)) {
    throw new RangeError(`factor must be a finite number of at least 1, got ${String(factor)}`)
  }
  if (!(typeof jitter === "number" && jitter >= 0 && jitter <= 1)) {
    throw new RangeError(`jitter must be a number from 0 to 1, got ${String(jitter)}`)
  }
  return attempt => {
    const delay = Math.min(base * factor ** (attempt - 1), max)
    // Jitter only shortens a delay, so that it never exceeds max and still spreads the retries that reached max.
    return jitter === 0 ? delay : delay * (1 - jitter * Math.random())
  }
}

/** Returns a backoff with the same delay in seconds after every attempt. */
export function fixedBackoff(seconds: number): Backoff {
  checkDelay("seconds", seconds)
  return () => seconds
}

/** Backoff of a task without one: 2 seconds after the first attempt, doubled after each next one, up to an hour. */
export const defaultBackoff: Backoff = exponentialBackoff()

function checkDelay(name: string, value: unknown) {
  if (!isDelay(value)) {
    throw new RangeError(`${name} must be ${delayRange}, got ${String(value)}`)
  }
}
