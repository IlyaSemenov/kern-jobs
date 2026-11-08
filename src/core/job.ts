// Rows of the job table and claims of active jobs.
import type { TextRow } from "./statement"

/** Key mode of an enqueue, stored in the row by its latest enqueue. */
export type KeyMode = "replace" | "keep" | "debounce"

/** Row of the job table; `bigint` columns come as strings, and a SQL NULL payload as `undefined`. */
export interface JobRow {
  id: string
  task: string
  scope: string
  queue: string | null
  key: string | null
  key_mode: KeyMode
  payload: unknown
  run_at: Date
  not_before: Date | null
  window_started_at: Date
  created_at: Date
  attempts: number
  max_attempts: number
  state: "pending" | "active" | "failed"
  claim_id: string | null
  locked_by: string | null
  locked_at: Date | null
  lease_until: Date | null
  last_error: string | null
  failed_at: Date | null
}

/** Claimed job with its claim id, which every heartbeat and settlement of the claim must match. */
export interface Claim {
  id: string
  claimId: string
  /** Claim lag in seconds: the claim time minus `run_at`. */
  lag: number
  job: JobRow
}

/** Reference to a claim, enough to heartbeat or settle it. */
export type ClaimRef = Pick<Claim, "id" | "claimId">

/**
 * SQL of the columns of job rows of table alias `t` that `toJob` reads.
 * Timestamps come as seconds since the epoch, so that reading them does not depend on `DateStyle` and `TimeZone`.
 */
export const jobColumns = (t: string) =>
  `${t}.id, ${t}.task, ${t}.scope, ${t}.queue, ${t}.key, ${t}.key_mode, ${t}.payload,
   extract(epoch from ${t}.run_at) as run_at, extract(epoch from ${t}.not_before) as not_before,
   extract(epoch from ${t}.window_started_at) as window_started_at, extract(epoch from ${t}.created_at) as created_at,
   ${t}.attempts, ${t}.max_attempts, ${t}.state, ${t}.claim_id, ${t}.locked_by,
   extract(epoch from ${t}.locked_at) as locked_at, extract(epoch from ${t}.lease_until) as lease_until, ${t}.last_error,
   extract(epoch from ${t}.failed_at) as failed_at`

/** Reads a job row selected with `jobColumns`. */
export function toJob(row: TextRow): JobRow {
  return {
    id: row.id!,
    task: row.task!,
    scope: row.scope!,
    queue: row.queue ?? null,
    key: row.key ?? null,
    key_mode: row.key_mode as KeyMode,
    payload: row.payload == null ? undefined : JSON.parse(row.payload),
    run_at: toTime(row.run_at)!,
    not_before: toTime(row.not_before),
    window_started_at: toTime(row.window_started_at)!,
    created_at: toTime(row.created_at)!,
    attempts: Number(row.attempts),
    max_attempts: Number(row.max_attempts),
    state: row.state as JobRow["state"],
    claim_id: row.claim_id ?? null,
    locked_by: row.locked_by ?? null,
    locked_at: toTime(row.locked_at),
    lease_until: toTime(row.lease_until),
    last_error: row.last_error ?? null,
    failed_at: toTime(row.failed_at),
  }
}

/** Reads a row returned by a claim statement: `jobColumns` and `lag_seconds`. */
export function toClaim(row: TextRow): Claim {
  return { id: row.id!, claimId: row.claim_id!, lag: Number(row.lag_seconds), job: toJob(row) }
}

function toTime(seconds: string | null | undefined): Date | null {
  return seconds == null ? null : new Date(Math.round(Number(seconds) * 1000))
}
