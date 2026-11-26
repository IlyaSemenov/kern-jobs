import { expect, test } from "bun:test"

import { BatchItemsError, RetryError, TerminalError } from "./errors"

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

test("BatchItemsError is an AggregateError of the failed items with their indexes in ascending order", () => {
  const [a, b] = [new Error("a"), "b"]
  const error = new BatchItemsError(
    new Map<number, unknown>([
      [3, b],
      [1, a],
    ]),
  )
  expect(error).toBeInstanceOf(AggregateError)
  expect([error.name, error.message, error.indexes, error.errors]).toEqual([
    "BatchItemsError",
    "2 batch items failed",
    [1, 3],
    [a, b],
  ])
  expect(Object.isFrozen(error.indexes)).toBe(true)
  const single = new BatchItemsError([[0, undefined]], "item 0 failed")
  expect([single.message, single.indexes, single.errors]).toEqual([
    "item 0 failed",
    [0],
    [undefined],
  ])
  expect(new BatchItemsError([[0, a]]).message).toBe("1 batch item failed")
})

test("BatchItemsError rejects no failed items, a repeated index, and an index that is not a non-negative integer", () => {
  expect(() => new BatchItemsError([])).toThrow(TypeError)
  expect(
    () =>
      new BatchItemsError([
        [1, "a"],
        [1, "b"],
      ]),
  ).toThrow(TypeError)
  // An index without a primitive value, whose conversion to a string throws, is rejected as any other.
  for (const index of [-1, 0.5, NaN, Infinity, 2 ** 53, "1", Object.create(null)]) {
    expect(() => new BatchItemsError([[index as number, "a"]])).toThrow(RangeError)
  }
})
