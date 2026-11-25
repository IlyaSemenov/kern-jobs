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
  runAt: t.epochTimestamp,
  notBefore: t.epochTimestamp.nullable(),
  windowStartedAt: t.epochTimestamp,
  createdAt: t.epochTimestamp,
  attempts: t.integer,
  maxAttempts: t.integer,
  state: t.text.of<"pending" | "active" | "failed">(),
  claimId: t.bigintText.nullable(),
  lockedBy: t.text.nullable(),
  lockedAt: t.epochTimestamp.nullable(),
  leaseUntil: t.epochTimestamp.nullable(),
  lastError: t.text.nullable(),
  failedAt: t.epochTimestamp.nullable(),
  // Attempt budget of the failure job that the claim of an active row stamped, or null for a claim without one; ignored on an inactive row.
  failureMaxAttempts: t.integer.nullable(),
})
