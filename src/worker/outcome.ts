// Outcome of a handler that ended, which a worker decides once, before it reports or settles it, the items that a partial failure of a batch keeps, and the message that a fail records.

import { BatchItemsError, RetryError, TerminalError } from "../task/errors"

import type { AbortReason, HandlerOutcome } from "./events"

/**
 * Classification of a value that a handler threw:
 * - `terminal`: a `TerminalError`;
 * - `retry`: a `RetryError`, with its delay as read from it;
 * - `items`: a `BatchItemsError`, with a copy of its report;
 * - `other`: any other value;
 * - with `failure`, what the inspection of the value threw: `instanceof` or the copy of the report of a `BatchItemsError`, which leave the type `other`, or the read of the delay of a `RetryError`, which stays `retry`.
 */
export type Thrown =
  | { type: "terminal" }
  | { type: "retry"; delay: unknown }
  | { type: "items"; report: BatchReport }
  | { type: "other" }
  | { type: "retry" | "other"; failure: unknown }

/** Copy of the report of a `BatchItemsError`, its indexes and the errors of its items in one order, or why the report is invalid. */
type BatchReport = { indexes: unknown[]; errors: unknown[] } | { invalid: string }

/** Partial failure of a batch: the ascending indexes of the items that its job keeps, the errors of those items, and the number of the items of the job. */
export interface BatchFailure {
  indexes: number[]
  errors: unknown[]
  length: number
}

/**
 * Classifies a value that a handler threw.
 * A worker classifies the value before it reports it, since `onError` can change a thrown object, and uses that classification for the outcome and the settlement.
 */
export function classifyThrown(error: unknown): Thrown {
  let type: "retry" | "other" = "other"
  try {
    if (error instanceof TerminalError) return { type: "terminal" }
    if (error instanceof BatchItemsError) return { type: "items", report: copyBatchReport(error) }
    if (!(error instanceof RetryError)) return { type: "other" }
    type = "retry"
    return { type, delay: error.delay }
  } catch (e) {
    // `instanceof`, the read of the delay, and the copy of a report run code of the thrown value, such as a trap of a proxy.
    return { type, failure: e }
  }
}

/**
 * Copies the indexes and the errors of a `BatchItemsError` into arrays of their own, each read once, before any other code of the application runs, so that neither `onError` nor a getter of a message read later changes the items that its job keeps.
 * The arrays of the error can be changed after its constructor, so the copy tells a hole of a sparse array, which is invalid, from an `undefined` error, which is not.
 */
function copyBatchReport(error: BatchItemsError): BatchReport {
  const indexes = copyDenseArray(error.indexes)
  const errors = copyDenseArray(error.errors)
  if (!indexes || !errors) return { invalid: "its indexes and errors are not arrays without holes" }
  if (indexes.length !== errors.length) {
    return { invalid: `it has ${indexes.length} indexes and ${errors.length} errors` }
  }
  return { indexes, errors }
}

/** Returns a copy of an array without holes, or `null` for another value; an element of its prototype is a hole. */
function copyDenseArray(value: unknown): unknown[] | null {
  if (!Array.isArray(value)) return null
  const copy: unknown[] = []
  for (let i = 0, length = value.length; i < length; i++) {
    if (!Object.hasOwn(value, i)) return null
    copy.push(value[i])
  }
  return copy
}

/**
 * Checks the copied report of a `BatchItemsError` against the claimed job, and returns the partial failure or why the report is invalid.
 * Only a batch job, whose row has `max_items`, keeps some of its items, whatever the implementation that runs it, since only its payload is an array of items that no other job holds.
 * An index must name an item of the payload as the worker passed it, by `length` read before any code of the application ran, and the indexes must be ascending and distinct, as the constructor sorts them.
 */
export function checkBatchReport(
  report: BatchReport,
  job: { isBatch: boolean; length: number | null },
): BatchFailure | { invalid: string } {
  if ("invalid" in report) return report
  if (!job.isBatch) return { invalid: "its job is not a batch job" }
  if (job.length === null) return { invalid: "the payload of its job is not an array" }
  const { indexes, errors } = report
  if (indexes.length === 0) return { invalid: "it names no item" }
  let previous = -1
  for (const index of indexes) {
    // The message names no index that is not a number, whose conversion to a string runs code of the application and can throw.
    if (typeof index !== "number" || !Number.isSafeInteger(index) || index <= previous) {
      return {
        invalid: "its indexes are not non-negative safe integers in strictly increasing order",
      }
    }
    if (index >= job.length) {
      return { invalid: `index ${index} is not an item of the ${job.length} items of its job` }
    }
    previous = index
  }
  return { indexes: indexes as number[], errors, length: job.length }
}

/**
 * Returns the error that a fail records for a partial failure of a batch: a summary of the counts and the attempt, then a line of the index and the message of each failed item, in the order of the items that the job keeps.
 * A message is a JSON string, so that a newline in it cannot pass for the line of another item; no item is copied, since the payload may hold data that does not belong in an error.
 */
export function formatBatchError(failure: BatchFailure, attempt: number): string {
  const lines = failure.indexes.map(
    (index, i) => `#${index}: ${JSON.stringify(readErrorMessage(failure.errors[i]))}`,
  )
  const items = failure.length === 1 ? "item" : "items"
  const summary = `${failure.indexes.length} of ${failure.length} batch ${items} failed in attempt ${attempt}`
  return [summary, ...lines].join("\n")
}

/** Returns the message of a thrown value, also of one whose inspection or string conversion throws, such as `Object.create(null)` or an object with a throwing getter. */
function readErrorMessage(error: unknown): string {
  try {
    return error instanceof Error ? String(error.message) : String(error)
  } catch {
    return "unprintable error"
  }
}

/**
 * Returns the message of a thrown value as a fail records it, as `readErrorMessage` reads it.
 * U+0000 becomes the text `\u0000`, since PostgreSQL rejects it in a text value, and the fail would never apply.
 */
export function toErrorMessage(error: unknown): string {
  return readErrorMessage(error).replaceAll("\0", "\\u0000")
}

/**
 * Decides what a worker does with a job whose handler ended.
 * A handler that returns completes its job, also after an abort.
 * A `TerminalError` and a `RetryError` decide the outcome also after an abort: the first fails the job for good, and the second spends the attempt.
 * Any other value thrown after the abort `stop`, also a `BatchItemsError`, is an interruption, also on the last attempt, since libraries wrap an abort in errors of their own; an abort `lost` or `deadline` changes nothing.
 * A failure on the last attempt is for good.
 */
export function toHandlerOutcome(end: {
  /** Classification of the thrown value, or `null` if the handler returned. */
  thrown: Thrown | null
  /** Reason of the abort of the signal of the handler by the time it ended, or `null`. */
  aborted: AbortReason | null
  /** Number of the attempt, from 1. */
  attempt: number
  maxAttempts: number
}): HandlerOutcome {
  const { thrown } = end
  if (!thrown) return "complete"
  if (end.aborted === "stop" && (thrown.type === "other" || thrown.type === "items")) {
    return "requeue"
  }
  return thrown.type === "terminal" || end.attempt >= end.maxAttempts ? "terminal" : "retry"
}
