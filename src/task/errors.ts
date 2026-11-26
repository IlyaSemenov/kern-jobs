// Errors that a handler throws to decide what happens to its job: a failure for good, a retry after a given delay, or a failure of some items of a batch.

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

/**
 * Error of the handler of a batch task that fails some of the items of its job: the job keeps only those items, and the others count as processed.
 * An index is the position of an item in the payload of the job in its order as the handler, or the `runHandler` of the worker, gets it, before any code changes the array.
 * The failed items run again after the backoff of the task, as an error of the whole job would run them, and the job fails for good with them on its last attempt; after the abort of its signal by the stop of the worker, the job returns at once with them, and its attempt does not count.
 */
export class BatchItemsError extends AggregateError {
  override readonly name = "BatchItemsError"
  /** Indexes of the failed items, ascending; `errors` gives their errors in the same order. */
  readonly indexes: readonly number[]

  /**
   * Takes the failed items as pairs of an index and the error of the item, such as a `Map` from indexes to errors.
   * It throws `TypeError` for no failed items or a repeated index, and `RangeError` for an index that is not a non-negative safe integer.
   */
  constructor(failures: Iterable<readonly [index: number, error: unknown]>, message?: string) {
    const entries = toBatchFailures(failures)
    super(
      entries.map(([, error]) => error),
      message ?? `${entries.length} batch ${entries.length === 1 ? "item" : "items"} failed`,
    )
    this.indexes = Object.freeze(entries.map(([index]) => index))
  }
}

/** Returns the pairs of a `BatchItemsError` in the ascending order of their indexes, after the checks of its constructor. */
function toBatchFailures(
  failures: Iterable<readonly [index: number, error: unknown]>,
): [number, unknown][] {
  const entries = Array.from(failures, ([index, error]): [number, unknown] => [index, error])
  if (entries.length === 0) throw new TypeError("BatchItemsError takes at least one failed item")
  const seen = new Set<number>()
  for (const [index] of entries) {
    // The message names no index that is not a number, whose conversion to a string can throw.
    if (typeof index !== "number" || !Number.isSafeInteger(index) || index < 0) {
      const got = typeof index === "number" ? String(index) : typeof index
      throw new RangeError(`the index of a failed item must be a non-negative integer, got ${got}`)
    }
    if (seen.has(index)) throw new TypeError(`the failed item ${index} is given twice`)
    seen.add(index)
  }
  return entries.sort(([a], [b]) => a - b)
}
