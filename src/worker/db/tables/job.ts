// Job table of the worker ORM schema.

import type { KeyMode } from "../../../sql/job"
import { defineTable } from "../base"
import * as t from "../types"

/** Job table, which typed worker queries use and hand-written statements select through `columnsOf`. */
export const job = defineTable("job", {
  id: t.bigintText.primaryKey(),
  task: t.text,
  scope: t.text,
  queue: t.text.nullable(),
  key: t.text.nullable(),
  keyMode: t.text.of<KeyMode>(),
  // SQL NULL is the payload of an enqueue that has no JSON text, such as `undefined`, and reaches the handler as `undefined`, apart from JSON `null`.
  payload: t.jsonb.nullableAs(() => undefined),
  runAt: t.epochMilliseconds,
  notBefore: t.epochMilliseconds.nullable(),
  windowStartedAt: t.epochMilliseconds,
  createdAt: t.epochMilliseconds,
  attempts: t.integer,
  maxAttempts: t.integer,
  state: t.text.of<"pending" | "active" | "failed">(),
  claimId: t.bigintText.nullable(),
  lockedBy: t.text.nullable(),
  lockedAt: t.epochMilliseconds.nullable(),
  leaseUntil: t.epochMilliseconds.nullable(),
  lastError: t.text.nullable(),
  failedAt: t.epochMilliseconds.nullable(),
  // Attempt budget of the failure job that the claim of an active row stamped, or null for a claim without one; ignored on an inactive row.
  failureMaxAttempts: t.integer.nullable(),
  // Capacity of a batch job, null for a job that is not a batch.
  maxItems: t.integer.nullable(),
  // Whether a pending batch job takes no more items, which takes it out of the unique index of the open keys.
  sealed: t.boolean,
})
