// Schedule table of the worker ORM schema: the cursor of each schedule.

import { defineTable } from "../base"
import * as t from "../types"

/** Schedule table, whose row keeps the time through which a schedule is evaluated. */
export const schedule = defineTable("schedule", {
  id: t.text.primaryKey(),
  // Not `epochTimestamp`, which would round the microseconds of the cursor: the check selects it as seconds since the epoch itself.
  evaluatedThrough: t.timestamptz,
})
