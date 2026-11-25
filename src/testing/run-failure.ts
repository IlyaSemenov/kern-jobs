// Run of the `onFailure` of a task implementation in a test, with a `JobFailure` built from the payload of the failed job.

import { decodePayload, encodePayload } from "../sql/job"
import { FAILURE_MESSAGES, type FailureReason, type JobFailure } from "../task/failure"
import type { FailureImplementation, TaskImplementation } from "../task/task"

import { resolveJobInfo, type RunJobInfo, runJobWithScope } from "./run-job"

/** Default error message of a failure whose reason a thrown error causes, such as `handler-error`. */
const DEFAULT_FAILURE_MESSAGE = "job failed"

/** Metadata of the failed job that replace those that `runFailure` gives, such as the attempt; a field given as `undefined` keeps its default. */
export type RunFailureJob<Scope extends string | null = string | null> = RunJobInfo<Scope> & {
  task?: string | undefined
}

/**
 * Options of `runFailure`, which replace the fields of the `JobFailure` that `onFailure` gets; `job` replaces those of `failure.job`, not of the context of `onFailure`, and a field given as `undefined` keeps its default.
 * For a task defined with `scoped: true`, whose `Scope` is `string`, they take the scope of the failed job.
 */
export type RunFailureOptions<Scope extends string | null = string | null> = {
  /** Signal of `onFailure`, which a test aborts to stop it; one that never aborts by default. */
  signal?: AbortSignal | undefined
  id?: string | undefined
  reason?: FailureReason | undefined
  /** Error of the failure, of which `onFailure` gets the message, such as of an `Error`. */
  error?: { message: string } | undefined
  /** Time of the failure, in ISO 8601 in UTC with milliseconds, such as `2030-01-01T00:00:00.000Z`. */
  failedAt?: string | undefined
} & (null extends Scope
  ? { job?: RunFailureJob<Scope> | undefined }
  : { job: RunFailureJob<Scope> & { scope: string } })

/** Arguments of `runFailure` after the implementation: a task defined with `scoped: true` takes the options with the scope of the failed job. */
// The arguments of a task that is not scoped are a union whose first member, with a payload, is not conditional on the payload type, so that a wrapper generic over the payload passes a payload and options without a cast.
export type RunFailureArgs<P, Scope extends string | null> = null extends Scope
  ?
      | [payload: P, options?: RunFailureOptions<Scope> | undefined]
      | (undefined extends P
          ? [payload?: P, options?: RunFailureOptions<Scope> | undefined]
          : never)
  : [payload: P, options: RunFailureOptions<Scope>]

/**
 * Runs the `onFailure` of a task implementation, as a worker runs it after a job of the task with the payload failed for good, and resolves to what `onFailure` returns, or rejects with what it throws.
 * The first argument of `onFailure` is a `JobFailure`: the id `"1"`, the reason `handler-error`, the error message that the worker records for the reason, or `job failed` for an error of the handler, the current time as `failedAt`, the payload of the failed job through JSON, as a worker reads it from the table, and as `job` the metadata of the failed job on its last attempt, as `runJob` gives them; `options` replaces them.
 * Its context is that of the failure job: the id after that of the failed job, the scope of the failed job, the first attempt, the attempts of `onFailure`, and no key or queue.
 * A task defined with `scoped: true` takes the scope of the failed job in `options.job.scope`; the job of another task has no scope unless `options.job.scope` gives one.
 * It rejects with `TypeError` for an implementation without `onFailure`, and with the errors of `runJob` for the options of the task, the scope, and `job.id`.
 */
export async function runFailure<P, Scope extends string | null>(
  implementation: TaskImplementation<string, P, Scope> & {
    readonly failure: FailureImplementation<string, P, Scope>
  },
  ...args: NoInfer<RunFailureArgs<P, Scope>>
): Promise<unknown> {
  const [payload, options] = args as [P, RunFailureOptions?]
  return await runFailureWithScope(implementation, payload, options, () => null)
}

/**
 * Runs `onFailure` as `runFailure` does, with the scope of `options.job`, or the scope that `getDefaultScope` returns when it is `undefined`.
 * It reads each field of `options` and `options.job` at most once, since a getter may return another value on each read.
 */
export async function runFailureWithScope<P>(
  implementation: TaskImplementation<string, P>,
  payload: P,
  options: RunFailureOptions = {},
  getDefaultScope: () => string | null,
): Promise<unknown> {
  const { task, failure } = implementation
  if (failure === undefined) {
    throw new TypeError(`Task ${task.name} has no onFailure`)
  }
  const { job = {}, signal, id, reason = "handler-error", error, failedAt } = options
  // The failed job takes the metadata of a job of the payload on its last attempt, which a failure for good of a job that did not fail by a TerminalError has.
  const info = resolveJobInfo(task, payload, job, getDefaultScope, maxAttempts => maxAttempts)
  const { task: jobTask } = job
  const jobFailure: JobFailure<P> = {
    id: id ?? "1",
    reason,
    // Only the message, which the table stores, since JSON drops the message of an `Error`, which is not enumerable.
    error: { message: error === undefined ? getFailureMessage(reason) : error.message },
    failedAt: failedAt ?? new Date().toISOString(),
    job: { ...info, task: jobTask ?? task.name },
    // The payload as the table stores it: an enqueue serializes it as the root of its JSON, not as a field of the failure.
    payload: decodePayload(encodePayload(payload)) as P,
  }
  // The failure job has the scope of the failed job, and the id after its id, since the identity of the table gives the failure job, inserted later, a greater one; `runJobWithScope` passes the failure through JSON.
  const failureJob = { id: String(BigInt(info.id) + 1n), scope: info.scope }
  return await runJobWithScope(failure, jobFailure, { signal, job: failureJob }, () => info.scope)
}

/** Returns the error message that the worker or the reaper records for a failure of a reason, or `job failed` for a reason that a thrown error causes. */
function getFailureMessage(reason: FailureReason): string {
  const messages: Partial<Record<FailureReason, string>> = FAILURE_MESSAGES
  return messages[reason] ?? DEFAULT_FAILURE_MESSAGE
}
