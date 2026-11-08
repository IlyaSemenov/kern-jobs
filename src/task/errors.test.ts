import { expect, test } from "bun:test"

import { RetryError, TerminalError } from "./errors"

test("TerminalError is an Error with its message and cause", () => {
  const cause = new Error("parse")
  const error = new TerminalError("bad payload", { cause })
  expect(error).toBeInstanceOf(Error)
  expect([error.name, error.message, error.cause]).toEqual(["TerminalError", "bad payload", cause])
})

test("RetryError is an Error with its message, cause, and delay", () => {
  const cause = new Error("429")
  const error = new RetryError("rate limited", { delay: 30, cause })
  expect(error).toBeInstanceOf(Error)
  expect([error.name, error.message, error.cause, error.delay]).toEqual([
    "RetryError",
    "rate limited",
    cause,
    30,
  ])
  expect(new RetryError("now", { delay: 0 }).delay).toBe(0)
})

test("RetryError rejects a delay that is not a number of seconds from 0 to 1e12", () => {
  for (const delay of [-1, NaN, Infinity, 1e20, "30"]) {
    expect(() => new RetryError("later", { delay: delay as number })).toThrow(RangeError)
  }
  // As a JavaScript caller passes it, past the types.
  expect(() => new RetryError("later", undefined as never)).toThrow(RangeError)
})
