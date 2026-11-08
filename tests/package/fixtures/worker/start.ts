// Start of the worker of the consumer and helpers of its handlers, exported with inferred types that their declarations name through the exports of kern-jobs; the type tests of `declaration-consumer/` check the emitted types.
import type { HandlerContext } from "kern-jobs"
import { type Driver, HandlerAbort, startWorker, type WorkerOptions } from "kern-jobs/worker"

import { manifest } from "./manifest.ts"

/** Starts the worker of the consumer, passing its optional settings through as they are. */
export function startConsumerWorker(
  driver: Driver,
  schemaName?: string,
  concurrency?: number,
  onError?: WorkerOptions["onError"],
) {
  return startWorker(driver, { manifest, schemaName, concurrency, pollInterval: 0.05, onError })
}

/** Returns the metadata of the job of a handler. */
export function describeJob(context: HandlerContext) {
  return context.job
}

/** Returns why the signal of a handler was aborted, or `null`. */
export function describeAbort({ signal }: HandlerContext) {
  return signal.reason instanceof HandlerAbort ? signal.reason.reason : null
}
