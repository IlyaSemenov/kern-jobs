import { describe, test } from "bun:test"

import type { EnqueueOptions, EnqueueOverrides } from "./options"

declare const date: Date

describe("EnqueueOptions", () => {
  test("takes the options of each key mode", () => {
    const options: EnqueueOptions[] = [
      { key: "k", queue: "q", maxAttempts: 3, delay: 5, resetAttempts: true },
      { key: "k", keyMode: "replace", runAt: date },
      { key: "k", keyMode: "keep", runAt: date },
      { key: "k", keyMode: "debounce", delay: 2, maxWait: 10, resetAttempts: true },
      { key: null, queue: null },
    ]
    void options
  })

  test("rejects both delay and runAt", () => {
    // @ts-expect-error delay and runAt exclude each other.
    const options: EnqueueOptions = { delay: 1, runAt: date }
    void options
  })

  test("rejects debounce without delay or maxWait, or with runAt", () => {
    const options: EnqueueOptions[] = [
      // @ts-expect-error debounce needs maxWait.
      { key: "k", keyMode: "debounce", delay: 2 },
      // @ts-expect-error debounce needs delay.
      { key: "k", keyMode: "debounce", maxWait: 10 },
      // @ts-expect-error debounce computes the run time from delay.
      { key: "k", keyMode: "debounce", delay: 2, maxWait: 10, runAt: date },
    ]
    void options
  })

  test("rejects maxWait outside debounce", () => {
    const options: EnqueueOptions[] = [
      // @ts-expect-error maxWait belongs to debounce.
      { key: "k", maxWait: 10 },
      // @ts-expect-error maxWait belongs to debounce.
      { key: "k", keyMode: "keep", maxWait: 10 },
    ]
    void options
  })

  test("rejects resetAttempts with keep", () => {
    // @ts-expect-error keep does not change the pending job.
    const options: EnqueueOptions = { key: "k", keyMode: "keep", resetAttempts: true }
    void options
  })
})

describe("EnqueueOverrides", () => {
  test("takes a part of the debounce options", () => {
    const overrides: EnqueueOverrides[] = [
      { maxWait: 20 },
      { delay: 5 },
      { keyMode: "debounce" },
      { keyMode: "replace", runAt: date },
    ]
    void overrides
  })

  test("takes undefined for any option, which keeps the option of the task", () => {
    const overrides: EnqueueOverrides = {
      key: undefined,
      queue: undefined,
      maxAttempts: undefined,
      delay: undefined,
      runAt: undefined,
      keyMode: undefined,
      resetAttempts: undefined,
      maxWait: undefined,
    }
    void overrides
  })

  test("rejects the combinations that the options reject", () => {
    const overrides: EnqueueOverrides[] = [
      // @ts-expect-error delay and runAt exclude each other.
      { delay: 1, runAt: date },
      // @ts-expect-error maxWait belongs to debounce.
      { keyMode: "replace", maxWait: 10 },
      // @ts-expect-error debounce computes the run time from delay.
      { keyMode: "debounce", runAt: date },
      // @ts-expect-error keep does not change the pending job.
      { keyMode: "keep", resetAttempts: true },
    ]
    void overrides
  })
})
