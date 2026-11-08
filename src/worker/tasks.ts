// Tasks of a worker from its manifest: each implementation runs a claimed job with its payload and metadata, under the lease and backoff of its task.
import type { JobRow } from "../core/job"
import { TerminalError } from "../task/errors"
import { type Manifest, toTaskMap } from "../task/manifest"
import { isLease, type JobInfo, leaseRange } from "../task/task"

import type { WorkerTask } from "./runtime"

/**
 * Returns the tasks of a worker by name from a manifest.
 * It checks the manifest and the settings of each task again, as `defineManifest` and `defineTask` do, since any object of their shape passes the types: it throws `RangeError` for a lease outside its range and `TypeError` for a backoff that is not a function or a `scoped` that is not a boolean.
 * A job of a scoped task without a scope, such as one enqueued before the task became scoped, does not run the handler, whose `job.scope` is typed as a string: it throws a `TerminalError`, so the job becomes failed without retries.
 */
export function toWorkerTasks(manifest: Manifest): Record<string, WorkerTask> {
  // `Object.fromEntries` defines own properties, so that a task named `__proto__` does not set the prototype.
  return Object.fromEntries(
    [...toTaskMap(manifest?.tasks)].map(([name, implementation]): [string, WorkerTask] => {
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
            return implementation.handler(job.payload, { signal, job: toJobInfo(job) })
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
