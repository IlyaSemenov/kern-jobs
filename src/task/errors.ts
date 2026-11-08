// Errors that a handler throws to decide what happens to its job: a failure for good, or a retry after a given delay.
import { delayRange, isDelay } from "./delay"

/** Error of a handler that fails its job for good, without retries, such as for a payload that cannot be processed. */
export class TerminalError extends Error {
  override readonly name = "TerminalError"
}

/** Options of a `RetryError`. */
export interface RetryErrorOptions extends ErrorOptions {
  /** Seconds until the retry, instead of the backoff of the task. */
  delay: number
}

/**
 * Error of a handler that retries its job after a given delay, such as the one of a `Retry-After` header.
 * The failed attempt counts, so a job on its last attempt fails for good.
 */
export class RetryError extends Error {
  override readonly name = "RetryError"
  /** Seconds until the retry. */
  readonly delay: number

  constructor(message: string, options: RetryErrorOptions) {
    super(message, options)
    if (!isDelay(options?.delay)) {
      throw new RangeError(`delay must be ${delayRange}, got ${String(options?.delay)}`)
    }
    this.delay = options.delay
  }
}
