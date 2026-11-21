// Outcome of a handler that ended: the classification of what it threw, completion, retries and failures for good, interruption by stop, and thrown values whose inspection throws.
import { describe, expect, test } from "bun:test"

import { RetryError, TerminalError } from "../task/errors"

import type { AbortReason } from "./events"
import { classifyThrown, toHandlerOutcome } from "./outcome"

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
    new Error("boom"),
    undefined,
    hostile(trap),
    retryErrorWithoutDelay(trap),
  ].map(classifyThrown)
  expect(thrown).toEqual([
    { type: "terminal" },
    { type: "retry", delay: 60 },
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

describe("a handler that throws after the abort of stop", () => {
  test("is interrupted, also on the last attempt and with a value whose class cannot be told", () => {
    expect(toOutcomeOf(new Error("boom"), { aborted: "stop" })).toBe("requeue")
    expect(toOutcomeOf(new Error("boom"), { aborted: "stop", last: true })).toBe("requeue")
    expect(toOutcomeOf(hostile(), { aborted: "stop", last: true })).toBe("requeue")
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
