// Outcome of a handler that ended: the classification of what it threw, completion, retries and failures for good, partial failures of batches and their recorded error, interruption by stop, and thrown values whose inspection throws.

import { describe, expect, test } from "bun:test"

import { BatchItemsError, RetryError, TerminalError } from "../task/errors"

import type { AbortReason } from "./events"
import {
  type BatchFailure,
  checkBatchReport,
  classifyThrown,
  formatBatchError,
  type Thrown,
  toHandlerOutcome,
} from "./outcome"

/** Returns the outcome of a handler that threw `error` on an attempt that is the last one or not. */
function toOutcomeOf(error: unknown, o: { aborted?: AbortReason; last?: boolean } = {}) {
  return toHandlerOutcome({
    thrown: classifyThrown(error),
    aborted: o.aborted ?? null,
    attempt: o.last ? 3 : 2,
    maxAttempts: 3,
  })
}

const retryError = () => new RetryError("rate limited", { delay: 60 })

/** Thrown value whose prototype `instanceof` cannot read: reading it throws `trap`. */
const hostile = (trap = new Error("trap")) =>
  new Proxy(
    {},
    {
      getPrototypeOf() {
        throw trap
      },
    },
  )

/** `RetryError` whose delay cannot be read: reading it throws `trap`. */
const retryErrorWithoutDelay = (trap = new Error("trap")) =>
  new Proxy(retryError(), {
    get(target, property, receiver) {
      if (property === "delay") throw trap
      return Reflect.get(target, property, receiver) as unknown
    },
  })

test("classifies thrown values, and captures the delay of a RetryError and what the inspection of a value threw", () => {
  const trap = new Error("trap")
  const thrown = [
    new TerminalError("bad payload"),
    retryError(),
    new BatchItemsError([[1, "boom"]]),
    new Error("boom"),
    undefined,
    hostile(trap),
    retryErrorWithoutDelay(trap),
  ].map(classifyThrown)
  expect(thrown).toEqual([
    { type: "terminal" },
    { type: "retry", delay: 60 },
    { type: "items", report: { indexes: [1], errors: ["boom"] } },
    { type: "other" },
    { type: "other" },
    { type: "other", failure: trap },
    { type: "retry", failure: trap },
  ])
})

test("a handler that returns completes its job, also after an abort and on the last attempt", () => {
  for (const aborted of [null, "stop", "lost", "deadline"] as const) {
    expect(toHandlerOutcome({ thrown: null, aborted, attempt: 3, maxAttempts: 3 })).toBe("complete")
  }
})

describe("a handler that throws", () => {
  test("retries its job while attempts are left, and fails it for good on the last attempt, also beyond a lowered budget", () => {
    expect(toOutcomeOf(new Error("boom"))).toBe("retry")
    expect(toOutcomeOf(new Error("boom"), { last: true })).toBe("terminal")
    const beyond = { thrown: classifyThrown("boom"), aborted: null, attempt: 4, maxAttempts: 3 }
    expect(toHandlerOutcome(beyond)).toBe("terminal")
  })

  test("fails its job for good with a TerminalError on any attempt", () => {
    expect(toOutcomeOf(new TerminalError("bad payload"))).toBe("terminal")
    expect(toOutcomeOf(new TerminalError("bad payload"), { last: true })).toBe("terminal")
  })

  test("spends the attempt with a RetryError, so the last one fails the job for good", () => {
    expect(toOutcomeOf(retryError())).toBe("retry")
    expect(toOutcomeOf(retryError(), { last: true })).toBe("terminal")
  })

  test("keeps its outcome after an abort that is not of stop", () => {
    for (const aborted of ["lost", "deadline"] as const) {
      expect(toOutcomeOf(new Error("boom"), { aborted })).toBe("retry")
      expect(toOutcomeOf(new Error("boom"), { aborted, last: true })).toBe("terminal")
    }
  })

  test("takes a value whose class cannot be told as an ordinary error", () => {
    expect(toOutcomeOf(hostile())).toBe("retry")
    expect(toOutcomeOf(hostile(), { last: true })).toBe("terminal")
  })
})

describe("a partial failure of a batch", () => {
  /** Returns the partial failure that a `BatchItemsError` reports for a batch job of `length` items, or why it is invalid. */
  const toFailureOf = (error: unknown, length: number | null = 5, isBatch = true) => {
    const thrown = classifyThrown(error)
    if (thrown.type !== "items") throw new Error(`classified as ${thrown.type}`)
    return checkBatchReport(thrown.report, { isBatch, length })
  }

  /** Replaces the frozen indexes of a `BatchItemsError`, as code of the application can. */
  const setIndexes = (error: BatchItemsError, indexes: unknown) =>
    Object.defineProperty(error, "indexes", { value: indexes })

  test("copies the indexes and the errors of its report, which later changes of the error do not reach", () => {
    const [a, b] = [new Error("a"), new Error("b")]
    const error = new BatchItemsError([
      [0, a],
      [2, b],
    ])
    const thrown = classifyThrown(error)
    setIndexes(error, [])
    error.errors.reverse()
    expect(thrown).toEqual({ type: "items", report: { indexes: [0, 2], errors: [a, b] } })
  })

  test("copies the indexes before the message of an error is read, so a getter of a message cannot change them", () => {
    const error = new BatchItemsError([
      [0, "x"],
      [2, "y"],
    ])
    // A mutable array of the indexes, which the getter changes in place.
    const indexes = [0, 2]
    setIndexes(error, indexes)
    error.errors[0] = {
      get message() {
        indexes[1] = 1
        return "changed"
      },
    }
    Object.setPrototypeOf(error.errors[0], Error.prototype)
    const failure = toFailureOf(error) as BatchFailure
    expect(failure.indexes).toEqual([0, 2])
    expect(formatBatchError(failure, 1)).toBe(
      '2 of 5 batch items failed in attempt 1\n#0: "changed"\n#2: "y"',
    )
    expect(failure.indexes).toEqual([0, 2])
  })

  test("takes an undefined error of an item, and keeps the number of items of the job", () => {
    expect(toFailureOf(new BatchItemsError([[4, undefined]]))).toEqual({
      indexes: [4],
      errors: [undefined],
      length: 5,
    })
  })

  test("is invalid for a job that is not a batch job or whose payload is not an array", () => {
    const error = new BatchItemsError([[0, "boom"]])
    expect(toFailureOf(error, 5, false)).toEqual({ invalid: "its job is not a batch job" })
    expect(toFailureOf(error, null)).toEqual({ invalid: "the payload of its job is not an array" })
  })

  test("is invalid for indexes or errors changed after its constructor", () => {
    // Indexes of two holes, whose prototype has elements.
    const inherited: number[] = new Array<number>(2)
    Object.setPrototypeOf(
      inherited,
      Object.assign(Object.create(Array.prototype) as object, [0, 2]),
    )
    const changed = (change: (error: BatchItemsError) => void) => {
      const error = new BatchItemsError([
        [0, "a"],
        [2, "b"],
      ])
      change(error)
      const failure = toFailureOf(error)
      return "invalid" in failure ? failure.invalid : failure
    }
    expect([
      changed(e => setIndexes(e, [])),
      changed(e => setIndexes(e, [-1, 2])),
      changed(e => setIndexes(e, [0, 5])),
      changed(e => setIndexes(e, [2, 0])),
      changed(e => setIndexes(e, [0, 0])),
      changed(e => setIndexes(e, [0, 1.5])),
      changed(e => e.errors.pop()),
      changed(e => delete e.errors[0]),
      changed(e => setIndexes(e, "0,2")),
      changed(e => setIndexes(e, [0, Object.create(null)])),
      changed(e => setIndexes(e, inherited)),
    ]).toEqual([
      "it has 0 indexes and 2 errors",
      "its indexes are not non-negative safe integers in strictly increasing order",
      "index 5 is not an item of the 5 items of its job",
      "its indexes are not non-negative safe integers in strictly increasing order",
      "its indexes are not non-negative safe integers in strictly increasing order",
      "its indexes are not non-negative safe integers in strictly increasing order",
      "it has 2 indexes and 1 errors",
      "its indexes and errors are not arrays without holes",
      "its indexes and errors are not arrays without holes",
      "its indexes are not non-negative safe integers in strictly increasing order",
      "its indexes and errors are not arrays without holes",
    ])
    const empty = new BatchItemsError([[0, "a"]])
    setIndexes(empty, [])
    empty.errors.length = 0
    expect(toFailureOf(empty)).toEqual({ invalid: "it names no item" })
  })

  test("is classified as an ordinary error, with what threw, when copying its report throws", () => {
    const trap = new Error("trap")
    const error = new BatchItemsError([[0, "a"]])
    Object.defineProperty(error, "indexes", {
      get() {
        throw trap
      },
    })
    expect(classifyThrown(error)).toEqual({ type: "other", failure: trap })
  })

  test("records the index and the message of each failed item as a JSON string, after a summary of the counts and the attempt", () => {
    const unprintable = Object.create(null) as object
    const failure: BatchFailure = {
      indexes: [1, 3, 4],
      errors: [new Error('line\n#3: "fake"'), unprintable, "nul \0 here"],
      length: 7,
    }
    expect(formatBatchError({ indexes: [0], errors: ["boom"], length: 1 }, 1)).toBe(
      '1 of 1 batch item failed in attempt 1\n#0: "boom"',
    )
    expect(formatBatchError(failure, 2).split("\n")).toEqual([
      "3 of 7 batch items failed in attempt 2",
      String.raw`#1: "line\n#3: \"fake\""`,
      `#3: "unprintable error"`,
      String.raw`#4: "nul \u0000 here"`,
    ])
  })

  test("retries its job while attempts are left, and fails it for good on the last attempt", () => {
    const error = new BatchItemsError([[0, "boom"]])
    expect(toOutcomeOf(error)).toBe("retry")
    expect(toOutcomeOf(error, { last: true })).toBe("terminal")
    expect(toOutcomeOf(error, { aborted: "lost" })).toBe("retry")
  })
})

describe("a handler that throws after the abort of stop", () => {
  test("is interrupted, also on the last attempt and with a value whose class cannot be told", () => {
    expect(toOutcomeOf(new Error("boom"), { aborted: "stop" })).toBe("requeue")
    expect(toOutcomeOf(new Error("boom"), { aborted: "stop", last: true })).toBe("requeue")
    expect(toOutcomeOf(hostile(), { aborted: "stop", last: true })).toBe("requeue")
  })

  test("is interrupted with a partial failure of a batch, also on the last attempt", () => {
    const thrown: Thrown = { type: "items", report: { indexes: [0], errors: ["aborted"] } }
    const end = { thrown, aborted: "stop", maxAttempts: 3 } as const
    expect(toHandlerOutcome({ ...end, attempt: 2 })).toBe("requeue")
    expect(toHandlerOutcome({ ...end, attempt: 3 })).toBe("requeue")
  })

  test("decides the outcome with a TerminalError or a RetryError", () => {
    expect(toOutcomeOf(new TerminalError("gone"), { aborted: "stop" })).toBe("terminal")
    expect(toOutcomeOf(retryError(), { aborted: "stop" })).toBe("retry")
    expect(toOutcomeOf(retryError(), { aborted: "stop", last: true })).toBe("terminal")
    // A RetryError whose delay cannot be read is still a RetryError.
    expect(toOutcomeOf(retryErrorWithoutDelay(), { aborted: "stop" })).toBe("retry")
    expect(toOutcomeOf(retryErrorWithoutDelay(), { aborted: "stop", last: true })).toBe("terminal")
  })
})
