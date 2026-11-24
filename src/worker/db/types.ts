// Column types of the worker: those of kern-orm and its own, which the modules of `worker/db/` import as `t` from here.
import { defineType, equalityOperators } from "kern-orm"

export * from "kern-orm/types"

/** `bigint` as its decimal text, which keeps values beyond `Number.MAX_SAFE_INTEGER`, compared by equality. */
export const bigintText = defineType({ sqlType: "bigint" }).withOperators(equalityOperators)

/** `timestamptz` read as seconds since the epoch, whose text depends neither on `DateStyle` nor on `TimeZone`, and decoded to a `Date` of those seconds as a JavaScript number times 1000, rounded to an integer. */
export const epochTimestamp = defineType({
  sqlType: "timestamptz",
  read: value => `extract(epoch from ${value})`,
}).transform({ decode: seconds => new Date(Math.round(Number(seconds) * 1000)) })
