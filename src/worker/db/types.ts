// Column types of the worker: those of kern-orm and its own, which the modules of `worker/db/` import as `t` from here.

import { defineType, equalityOperators } from "kern-orm"
import { timestamptzPortable } from "kern-orm/types"

export * from "kern-orm/types"

/** `bigint` as its decimal text, which keeps values beyond `Number.MAX_SAFE_INTEGER`, compared by equality. */
export const bigintText = defineType({ sqlType: "bigint" }).withOperators(equalityOperators)

/** `timestamptz` read independently of the session settings as milliseconds since the epoch, truncated; accepts a `Date` too. */
export const epochMilliseconds = timestamptzPortable.transform({ decode: Date.parse })
