// Tasks of a worker from the task implementations of its manifest: each implementation runs a claimed job with its payload and metadata, under the lease and backoff of its task, through the `runHandler` of the worker.
import type { JobRow } from "../core/job"
import { TerminalError } from "../task/errors"
import { toTaskMap } from "../task/manifest"
import {
  checkTaskName,
  type HandlerContext,
  isLease,
  type JobInfo,
  leaseRange,
  type TaskImplementation,
} from "../task/task"

import type { WorkerTask } from "./runtime"

/** What `runHandler` of a worker gets about a job whose handler it runs: the context of the handler, the name of the task, and the payload of the job. */
export interface RunHandlerContext extends HandlerContext {
  task: string
  payload: unknown
}

/**
 * Wraps the handler of every job of a worker, such as to run it in the context of the scope of the job: `run` runs the handler and returns its promise, which rejects also when the handler throws synchronously.
 * The worker settles the job by the outcome of `runHandler` as by the outcome of a handler, also after the abort of its signal, so call `run` at most once and return or await its promise.
 * A `runHandler` that resolves without calling `run` completes the job without running its handler.
 */
export type RunHandler = (context: RunHandlerContext, run: () => Promise<unknown>) => unknown

/**
 * Returns the tasks of a worker by name from the task implementations of a manifest.
 * It checks the list, the name, and the settings of each task again, as `defineManifest` and `defineTask` do, since any object of their shape passes the types: it throws the errors of `defineTask` for a wrong name, `RangeError` for a lease outside its range, and `TypeError` for a backoff that is not a function or a `scoped` that is not a boolean.
 * A job of a scoped task without a scope, such as one enqueued before the task became scoped, does not run the handler, whose `job.scope` is typed as a string, nor `runHandler`: it throws a `TerminalError`, so the job becomes failed without retries.
 */
export function toWorkerTasks(
  implementations: readonly TaskImplementation<string, unknown>[],
  runHandler?: RunHandler,
): Record<string, WorkerTask> {
  // `Object.fromEntries` defines own properties, so that a task named `__proto__` does not set the prototype.
  return Object.fromEntries(
    [...toTaskMap(implementations)].map(([name, implementation]): [string, WorkerTask] => {
      checkTaskName(name)
      const { lease, backoff, scoped } = implementation.task
      if (lease !== undefined && !isLease(lease)) {
        throw new RangeError(`lease of task ${name} must be ${leaseRange}, got ${String(lease)}`)
      }
      if (backoff !== undefined && typeof backoff !== "function") {
        throw new TypeError(`backoff of task ${name} must be a function of the attempt`)
      }
      if (scoped !== undefined && typeof scoped !== "boolean") {
        throw new TypeError(`scoped of task ${name} must be a boolean`)
      }
      return [
        name,
        {
          handler: (job, { signal }) => {
            if (scoped && job.scope === "") {
              throw new TerminalError(`task ${name} is scoped, but its job has no scope`)
            }
            const context: HandlerContext = { signal, job: toJobInfo(job) }
            const run = async () => await implementation.handler(job.payload, context)
            return runHandler
              ? runHandler({ ...context, task: name, payload: job.payload }, run)
              : run()
          },
          lease,
          backoff,
        },
      ]
    }),
  )
}

/** Returns the metadata of a claimed job, as handlers and `onError` get it. */
export function toJobInfo(job: JobRow): JobInfo {
  return {
    id: job.id,
    // The table stores the empty string for a job without a scope, which `createJobs` rejects as a scope.
    scope: job.scope === "" ? null : job.scope,
    attempt: job.attempts,
    maxAttempts: job.max_attempts,
    key: job.key,
    queue: job.queue,
  }
}
