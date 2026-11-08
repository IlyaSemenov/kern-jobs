import { expect, test } from "bun:test"

import { isDelay } from "./delay"

test("isDelay accepts numbers of seconds from 0 to 1e12, which PostgreSQL adds to a timestamp", () => {
  expect([0, 1.5, 3600, 1e12].map(isDelay)).toEqual([true, true, true, true])
  expect([-1, 1e12 + 1, 1e20, NaN, Infinity, "1", null, undefined].map(isDelay)).toEqual(
    Array(8).fill(false),
  )
})
