// Settlement of a claim by the worker: complete, fail, or requeue, retried on a new connection while the worker owns the claim.
import { ConnectionError, type Database, isOutcomeUnknown } from "kern-db"

import type { Claim } from "../core/job"
import type { JobTable } from "../core/schema"
import {
  complete,
  fail,
  type FailOptions,
  type FailOutcome,
  requeue,
  type RequeueOutcome,
} from "../core/settlement"

import type { SettleOutcome, WorkerEvent } from "./events"
import type { KeptLease } from "./heartbeat"
import { monotonicNow, sleep } from "./loop"

/** Retries of a settlement that PostgreSQL rejected, such as by `statement_timeout`, before it is given up. */
const maxRejections = 3

/** Settlement of a claim: `complete`, `fail` with these options, or `requeue` with the attempt given back. */
export type SettleAction =
  | { type: "complete" }
  | { type: "fail"; options: FailOptions }
  | { type: "requeue" }

/** Options of a settlement. */
export interface SettleOptions {
  /** Database of the worker, whose pool runs complete and whose transaction runs fail. */
  database: Pick<Database, "execute" | "transaction">
  table: JobTable
  claim: Claim
  /** Lease of the claim, which the heartbeat keeps during the settlement. */
  lease: KeptLease
  action: SettleAction
  /** Seconds before the first retry, doubled up to `maxRetryDelay`. */
  retryDelay: number
  maxRetryDelay: number
  /** Whether the worker may still send a statement of the settlement; it is checked before each attempt. */
  proceed(): boolean
  onEvent(event: WorkerEvent): void
}

/**
 * Settles a claim, retrying after a connection error on a new connection while the lease of the claim is confirmed.
 * A retry is safe, since a settlement matches the claim id: after a statement whose outcome is unknown, a retry either applies the settlement or finds the claim gone, which gives `unknown`, as does giving up after such a statement.
 * A claim that a heartbeat found lost is not settled, and no attempt starts once `proceed` returns `false`, which gives `stopped`.
 */
export async function settle(o: SettleOptions): Promise<SettleOutcome> {
  let unknown = false
  let rejections = 0
  let delay = o.retryDelay
  for (;;) {
    if (o.lease.lost) return unknown ? "unknown" : "lost"
    if (!o.proceed()) return unknown ? "unknown" : "stopped"
    try {
      const outcome = await run(o)
      return outcome === "fenced" && unknown ? "unknown" : outcome
    } catch (error) {
      o.onEvent({ type: "settle-error", claim: o.claim, error })
      if (error instanceof ConnectionError) unknown ||= isOutcomeUnknown(error)
      else if (++rejections > maxRejections) return unknown ? "unknown" : "error"
    }
    // Past the confirmed lease, the reaper may return the job, and retries would only hold the slot of the worker.
    if (monotonicNow() >= o.lease.confirmedUntil) return unknown ? "unknown" : "abandoned"
    if (!o.proceed()) return unknown ? "unknown" : "stopped"
    await sleep(delay)
    delay = Math.min(delay * 2, o.maxRetryDelay)
  }
}

async function run(o: SettleOptions): Promise<"completed" | FailOutcome | RequeueOutcome> {
  switch (o.action.type) {
    case "complete":
      return (await complete(o.database, o.table, o.claim)) ? "completed" : "fenced"
    case "fail":
      return await fail(o.database, o.table, o.claim, o.action.options)
    case "requeue":
      return await requeue(o.database, o.table, o.claim)
  }
}
