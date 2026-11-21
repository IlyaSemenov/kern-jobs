// Failures of jobs for good, as the `onFailure` of a task implementation gets them: the `JobFailure` that a failure job has as its payload.
import type { FailureReason } from "../core/failure"

export type { FailureReason }

/** Recorded failure for good of a job, which `onFailure` gets. */
export interface JobFailure<Payload = unknown> {
  /** Id of the failure, the same in each run of `onFailure` for it, unique within the installation; a job that fails for good again after a retry gets another one. */
  id: string
  /**
   * Why the job failed for good:
   * - `handler-error`: its handler, or the `runHandler` of its worker, threw on its last attempt, also a `RetryError`;
   * - `terminal-error`: its handler, or `runHandler`, threw a `TerminalError`, or it was of a scoped task without a scope;
   * - `lease-expired`: its lease expired on its last attempt, such as after its worker crashed;
   * - `worker-restarted`: a worker that started with the id of its worker returned it on its last attempt;
   * - `late-claim`: it was taken too late to keep its lease on its last attempt, and its handler did not run in that attempt.
   */
  reason: FailureReason
  /** Error of the failure: the message of what the handler threw, or what the worker or the reaper recorded. */
  error: { message: string }
  /** Time of the failure, in ISO 8601 in UTC with milliseconds, such as `2030-01-01T00:00:00.000Z`. */
  failedAt: string
  /** Metadata of the job that failed, as it was at the failure. */
  job: {
    id: string
    /** Name of the task of the job. */
    task: string
    /** Scope of the job, or `null` for a job without one, also for a task that is scoped now. */
    scope: string | null
    /** Number of the attempt that failed, from 1. */
    attempt: number
    maxAttempts: number
    key: string | null
    queue: string | null
  }
  /** Payload of the job as it was stored when it failed, `undefined` for a job enqueued without one. */
  payload: Payload
}
