// Tasks of a worker from the task implementations of its manifest: each implementation runs a claimed job with its payload and metadata, under the lease and backoff of its task, through the `runHandler` of the worker, and an implementation with a failure task gives the attempt budget of failure jobs that its claims stamp and the task that runs its failure jobs.

import { TerminalError } from "../task/errors"
import { type ManifestTask, toManifestTasks } from "../task/manifest"
import { type RunHandler, runTaskHandler } from "../task/run-handler"
import {
  checkTaskName,
  isLease,
  type JobInfo,
  leaseRange,
  type TaskImplementation,
} from "../task/task"

import type { ClaimedJob } from "./db/claim"
import type { WorkerTask } from "./runtime"

/**
 * Returns the tasks of a worker by name from the task implementations of a manifest: the task of each implementation, with the attempt budget of failure jobs that its claims stamp, and the failure task of an implementation that has one.
 * It checks the list, the name, the settings, and the failure task of each task again, as `defineManifest`, `defineTask`, and `implement` do, since any object of their shape passes the types: it throws the errors of `defineManifest` for the list, those of `defineTask` for a wrong name, `RangeError` for a lease outside its range, and `TypeError` for a backoff that is not a function or a `scoped` that is not a boolean.
 * A job of a scoped task without a scope, such as one enqueued before the task became scoped, does not run the handler, whose `job.scope` is typed as a string, nor `runHandler`: it throws a `TerminalError`, so the job becomes failed without retries.
 */
export function toWorkerTasks(
  implementations: readonly TaskImplementation<string, unknown>[],
  runHandler?: RunHandler,
): Record<string, WorkerTask> {
  // `Object.fromEntries` defines own properties, so that a task named `__proto__` does not set the prototype.
  return Object.fromEntries(
    toManifestTasks(implementations).map(task => [task.name, toWorkerTask(task, runHandler)]),
  )
}

/** Returns the task of a worker that runs the jobs of a task of a manifest, and checks its name and settings as `toWorkerTasks` describes. */
function toWorkerTask(task: ManifestTask, runHandler: RunHandler | undefined): WorkerTask {
  const { name, implementation, failureMaxAttempts } = task
  const { lease, backoff, scoped } = task.settings
  checkTaskName(name)
  if (lease !== undefined && !isLease(lease)) {
    throw new RangeError(`lease of task ${name} must be ${leaseRange}, got ${String(lease)}`)
  }
  if (backoff !== undefined && typeof backoff !== "function") {
    throw new TypeError(`backoff of task ${name} must be a function of the attempt`)
  }
  if (scoped !== undefined && typeof scoped !== "boolean") {
    throw new TypeError(`scoped of task ${name} must be a boolean`)
  }
  return {
    handler: (job, { signal }) => {
      if (scoped && job.scope === "") {
        throw new TerminalError(`task ${name} is scoped, but its job has no scope`)
      }
      const context = { signal, job: toJobInfo(job), task: name, payload: job.payload }
      return runTaskHandler(implementation, context, runHandler)
    },
    lease,
    backoff,
    failureMaxAttempts,
  }
}

/** Returns the metadata of a claimed job, as handlers and `onError` get it. */
export function toJobInfo(job: ClaimedJob): JobInfo {
  return {
    id: job.id,
    // The table stores the empty string for a job without a scope, which `createJobs` rejects as a scope.
    scope: job.scope === "" ? null : job.scope,
    attempt: job.attempts,
    maxAttempts: job.maxAttempts,
    key: job.key,
    queue: job.queue,
  }
}
