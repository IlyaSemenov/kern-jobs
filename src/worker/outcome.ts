// Outcome of a handler that ended, which a worker decides once, before it reports or settles it.
import { RetryError, TerminalError } from "../task/errors"

import type { AbortReason, HandlerOutcome } from "./events"

/**
 * Classification of a value that a handler threw:
 * - `terminal`: a `TerminalError`;
 * - `retry`: a `RetryError`, with its delay as read from it;
 * - `other`: any other value;
 * - with `failure`, what the inspection of the value threw: `instanceof`, which leaves the type `other`, or the read of the delay of a `RetryError`, which stays `retry`.
 */
export type Thrown =
  | { type: "terminal" }
  | { type: "retry"; delay: unknown }
  | { type: "other" }
  | { type: "retry" | "other"; failure: unknown }

/**
 * Classifies a value that a handler threw.
 * A worker classifies the value before it reports it, since `onError` can change a thrown object, and uses that classification for the outcome and the settlement.
 */
export function classifyThrown(error: unknown): Thrown {
  let type: "retry" | "other" = "other"
  try {
    if (error instanceof TerminalError) return { type: "terminal" }
    if (!(error instanceof RetryError)) return { type: "other" }
    type = "retry"
    return { type, delay: error.delay }
  } catch (e) {
    // `instanceof` and the read of the delay run code of the thrown value, such as a trap of a proxy.
    return { type, failure: e }
  }
}

/**
 * Decides what a worker does with a job whose handler ended.
 * A handler that returns completes its job, also after an abort.
 * A `TerminalError` and a `RetryError` decide the outcome also after an abort: the first fails the job for good, and the second spends the attempt.
 * Any other value thrown after the abort `stop` is an interruption, also on the last attempt, since libraries wrap an abort in errors of their own; an abort `lost` or `deadline` changes nothing.
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
  if (end.aborted === "stop" && thrown.type === "other") return "requeue"
  return thrown.type === "terminal" || end.attempt >= end.maxAttempts ? "terminal" : "retry"
}
