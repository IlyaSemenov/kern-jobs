// Run of a handler in a test, with the payload and the metadata that a worker would give a job of its task.

import { checkScope } from "../producer/jobs"
import { DEFAULT_MAX_ATTEMPTS } from "../sql/enqueue"
import { decodePayload, encodePayload } from "../sql/job"
import { getTaskEnqueueParams } from "../task/options"
import type { HandlerContext, JobInfo, TaskImplementation } from "../task/task"

/** Metadata of the job that replace those that `runJob` gives, such as the attempt; a field given as `undefined` keeps its default. */
export type RunJobInfo<Scope extends string | null = string | null> = {
  [K in keyof JobInfo<Scope>]?: JobInfo<Scope>[K] | undefined
}

/** Options of `runJob`; for a task defined with `scoped: true`, whose `Scope` is `string`, they take the scope of its job. */
export type RunJobOptions<Scope extends string | null = string | null> = {
  /** Signal of the handler, which a test aborts to stop it; one that never aborts by default. */
  signal?: AbortSignal | undefined
} & (null extends Scope
  ? { job?: RunJobInfo<Scope> | undefined }
  : { job: RunJobInfo<Scope> & { scope: string } })

/** Arguments of `runJob` after the implementation: a task defined with `scoped: true` takes the options with the scope of its job. */
// The arguments of a task that is not scoped are a union whose first member, with a payload, is not conditional on the payload type, so that a wrapper generic over the payload passes a payload and options without a cast.
export type RunJobArgs<P, Scope extends string | null> = null extends Scope
  ?
      | [payload: P, options?: RunJobOptions<Scope>]
      | (undefined extends P ? [payload?: P, options?: RunJobOptions<Scope>] : never)
  : [payload: P, options: RunJobOptions<Scope>]

/**
 * Runs the handler of a task implementation with a payload, as a worker runs a job of the task, and resolves to what the handler returns, or rejects with what it throws.
 * The handler gets the payload as a worker reads it from the table, through JSON, so a `Date` becomes a string, and the metadata of a job that an enqueue of this payload without overrides would add: the key, the queue, and `maxAttempts` of the options of the task, the first attempt, and the id `"1"`; `options.job` replaces them.
 * A task defined with `scoped: true` takes the scope of its job in `options.job.scope`; the job of another task has no scope unless `options.job.scope` gives one.
 * It rejects with the errors of an enqueue for the options of the task, `TypeError` for a scoped task without a scope, and the errors of the `scope` of `createJobs` for a scope that is not one.
 * It runs neither the `runHandler` of a worker nor its handling of `RetryError` and `TerminalError`.
 */
export async function runJob<P, Scope extends string | null>(
  implementation: TaskImplementation<string, P, Scope>,
  ...args: NoInfer<RunJobArgs<P, Scope>>
): Promise<unknown> {
  const [payload, options] = args as [P, RunJobOptions?]
  return await runJobWithScope(implementation, payload, options, () => null)
}

/**
 * Runs a handler as `runJob` does, with the scope of `options.job`, or the scope that `getDefaultScope` returns when it is `undefined`.
 * It reads each field of `options` and `options.job` once, since a getter may return another value on each read.
 */
export async function runJobWithScope<P>(
  implementation: TaskImplementation<string, P>,
  payload: P,
  options: RunJobOptions = {},
  getDefaultScope: () => string | null,
): Promise<unknown> {
  const { task } = implementation
  const { job = {}, signal = new AbortController().signal } = options
  const jobScope = job.scope
  const scope = jobScope === undefined ? getDefaultScope() : jobScope
  if (scope === null) {
    // A worker does not run the job of a scoped task without a scope, so its handler, typed with a string scope, never gets one.
    if (task.scoped === true) {
      throw new TypeError(`Task ${task.name} is scoped: pass the scope of its job as job.scope`)
    }
  } else {
    checkScope(scope)
  }
  const params = getTaskEnqueueParams(task, payload)
  // The other fields are read after the checks of the scope and the options of the task, so that they reject first.
  const { id, attempt, maxAttempts, key, queue } = job
  const info: JobInfo = {
    id: id ?? "1",
    scope,
    attempt: attempt ?? 1,
    maxAttempts: maxAttempts ?? params.maxAttempts ?? DEFAULT_MAX_ATTEMPTS,
    key: key === undefined ? (params.key ?? null) : key,
    queue: queue === undefined ? (params.queue ?? null) : queue,
  }
  const context: HandlerContext = { signal, job: info }
  return await implementation.handler(decodePayload(encodePayload(payload)) as P, context)
}
