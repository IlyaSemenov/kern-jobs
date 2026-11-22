// Events of a worker, which it reports synchronously to its `onEvent` for observation and tests.
import type { Claim, ClaimRef } from "./db/claim"
import type { Reaped } from "./db/lease"
import type { FailOutcome, RequeueOutcome } from "./db/settlement"

/**
 * Why the signal of a handler was aborted:
 * - `lost`: the worker no longer holds the job, such as after its lease expired, and another worker may run it;
 * - `deadline`: the worker could not extend the lease of the job, such as without a connection, and the lease is about to end;
 * - `stop`: the worker stops.
 */
export type AbortReason = "lost" | "deadline" | "stop"

/**
 * What a worker decides to do with a job once its handler ends, before it reports or settles it:
 * - `complete`: the handler returned;
 * - `requeue`: `stop` interrupted the handler, so the worker returns its job with the attempt given back;
 * - `retry`: the handler failed, and the job has attempts left, so the worker returns it for another one;
 * - `terminal`: the handler failed by a `TerminalError` or on the last attempt, so the worker fails its job for good.
 *
 * The settlement may record another result, such as a job superseded rather than retried, or none.
 */
export type HandlerOutcome = "complete" | "requeue" | "retry" | "terminal"

/**
 * Outcome of the settlement of a claim:
 * - `completed` and the outcomes of fail and requeue: applied;
 * - `fenced`: the settlement found the claim lost;
 * - `lost`: a heartbeat had found the claim lost, so the worker did not settle it;
 * - `unknown`: a statement failed with an unknown outcome, and no later statement settled the claim: a retry found the claim gone, or the worker gave the settlement up, so that statement may have been applied;
 * - `abandoned`: the settlement failed with connection errors until the worker could no longer confirm the lease, and nothing was applied;
 * - `error`: the settlement failed with rejected statements and was given up, and nothing was applied;
 * - `stopped`: the deadline of `stop` passed before the worker could settle the claim, and nothing was applied.
 *
 * After `abandoned`, `error`, and `stopped`, the job stays active until the reaper returns it.
 */
export type SettleOutcome =
  | "completed"
  | FailOutcome
  | RequeueOutcome
  | "lost"
  | "unknown"
  | "abandoned"
  | "error"
  | "stopped"

/** Event of a worker. */
export type WorkerEvent =
  | { type: "start"; claim: Claim }
  // The handler ended with `outcome`: it returned, or threw `error`; `aborted` is the reason of the abort of its signal by then.
  | {
      type: "end"
      claim: Claim
      outcome: HandlerOutcome
      error?: unknown
      aborted: AbortReason | null
    }
  | { type: "settled"; claim: Claim; outcome: SettleOutcome }
  // The claim returned after the deadline of the lease of its job; the worker fails the job without running its handler.
  | { type: "late-claim"; claim: Claim }
  // The backoff of the task threw or gave no delay in seconds; the job retries with the default backoff.
  | { type: "backoff-error"; claim: Claim; error: unknown }
  // A settlement statement failed; the worker retries it or gives up.
  | { type: "settle-error"; claim: Claim; error: unknown }
  | { type: "abort"; claim: ClaimRef; reason: AbortReason }
  // A heartbeat of these claim ids was sent.
  | { type: "heartbeat"; claims: string[] }
  | { type: "heartbeat-error"; error: unknown }
  // A heartbeat succeeded after a failed one.
  | { type: "heartbeat-recovered" }
  | { type: "claim-error"; error: unknown }
  // The check of the schema failed; the worker takes no jobs and checks again after the poll interval.
  | { type: "schema-error"; error: unknown }
  // Evaluating this schedule failed; the worker continues with the next schedule.
  | { type: "schedule-error"; id: string; error: unknown }
  // The driver failed to release a connection, or the lock of the worker id on it; the outcome of the statements on it stands.
  | { type: "release-error"; error: unknown }
  | { type: "reap"; jobs: Reaped[] }
  // At its start, the worker returned the jobs that a previous worker of its id had claimed.
  | { type: "recover"; jobs: Reaped[] }
  | { type: "reap-error"; error: unknown }
  // An unexpected failure of the worker, such as a defect; the worker goes on.
  | { type: "error"; error: unknown }
