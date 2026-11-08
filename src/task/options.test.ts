import { describe, expect, test } from "bun:test"

import {
  type EnqueueOptions,
  type EnqueueOverrides,
  mergeOptions,
  toEnqueueParams,
} from "./options"

const date = new Date(Date.UTC(2030, 0, 1))

describe("mergeOptions", () => {
  test("replaces the fields that the overrides give", () => {
    expect(
      mergeOptions({ key: "k", queue: "q", maxAttempts: 3 }, { queue: "other", maxAttempts: 5 }),
    ).toEqual({ key: "k", queue: "other", maxAttempts: 5 })
  })

  test("keeps a field that an override gives as undefined, and removes one that it gives as null", () => {
    expect(
      mergeOptions({ key: "k", queue: "q", delay: 5 }, { key: null, delay: undefined }),
    ).toEqual({
      key: null,
      queue: "q",
      delay: 5,
    })
  })

  test("drops runAt for an override of delay, and delay for an override of runAt", () => {
    expect(mergeOptions({ runAt: date }, { delay: 5 })).toEqual({ delay: 5 })
    expect(mergeOptions({ delay: 5 }, { runAt: date })).toEqual({
      runAt: date,
    })
  })

  test("merges several layers, each replacing the fields of the layers before it", () => {
    expect(
      mergeOptions(
        { key: "k", queue: "q", delay: 5 },
        { key: undefined, runAt: date },
        { queue: null, maxAttempts: 2 },
      ),
    ).toEqual({ key: "k", queue: null, runAt: date, maxAttempts: 2 })
  })

  test("takes the overrides alone for a task without options", () => {
    expect(mergeOptions(undefined, { key: "k" })).toEqual({ key: "k" })
    expect(mergeOptions(undefined, undefined)).toEqual({})
  })
})

describe("toEnqueueParams", () => {
  test("turns options into the parameters of the core", () => {
    expect(
      toEnqueueParams("t", { id: 1 }, { key: "k", queue: "q", maxAttempts: 3, delay: 5 }),
    ).toEqual({
      task: "t",
      payload: { id: 1 },
      key: "k",
      queue: "q",
      mode: "replace",
      runAt: undefined,
      delay: 5,
      maxWait: null,
      maxAttempts: 3,
      resetAttempts: undefined,
    })
  })

  test("passes maxWait in debounce mode only", () => {
    const debounce: EnqueueOptions = {
      key: "k",
      keyMode: "debounce",
      delay: 2,
      maxWait: 10,
    }
    expect(toEnqueueParams("t", null, debounce)).toMatchObject({
      mode: "debounce",
      maxWait: 10,
    })
    expect(
      toEnqueueParams("t", null, mergeOptions(debounce, { keyMode: "replace" })),
    ).toMatchObject({ mode: "replace", delay: 2, maxWait: null })
  })

  test("ignores an invalid maxWait outside debounce, and resetAttempts with keep", () => {
    const debounce: EnqueueOptions = {
      key: "k",
      keyMode: "debounce",
      delay: 2,
      maxWait: Number.POSITIVE_INFINITY,
      // As a JavaScript caller passes it, past the types.
      resetAttempts: Number.POSITIVE_INFINITY as unknown as boolean,
    }
    expect(toEnqueueParams("t", null, mergeOptions(debounce, { keyMode: "keep" }))).toMatchObject({
      mode: "keep",
      maxWait: null,
      resetAttempts: undefined,
    })
  })

  test("rejects the settings of a task, which only defineTask takes", () => {
    // As a JavaScript caller passes them, past the types.
    for (const setting of [{ lease: 5 }, { backoff: () => 1 }, { scoped: true }]) {
      const layer = { key: "k", ...setting } as unknown as EnqueueOverrides
      expect(() => toEnqueueParams("t", null, mergeOptions({ delay: 5 }, layer))).toThrow(
        "lease, backoff, and scoped are settings of defineTask, which an enqueue cannot override",
      )
    }
  })

  test("rejects delay with runAt, also of one overrides object in either order", () => {
    expect(() => toEnqueueParams("t", null, { delay: 1, runAt: date })).toThrow(TypeError)
    // As a JavaScript caller passes them, past the types.
    const conflicting = [
      { delay: 1, runAt: date },
      { runAt: date, delay: 1 },
    ] as unknown as EnqueueOverrides[]
    for (const overrides of conflicting) {
      expect(() => toEnqueueParams("t", null, mergeOptions({ delay: 5 }, overrides))).toThrow(
        TypeError,
      )
    }
  })

  test("rejects debounce without delay or maxWait, or with runAt", () => {
    for (const o of [
      { keyMode: "debounce", delay: 2 },
      { keyMode: "debounce", maxWait: 10 },
      { keyMode: "debounce", maxWait: 10, runAt: date },
    ] as const) {
      expect(() => toEnqueueParams("t", null, o)).toThrow(TypeError)
    }
  })

  test("accepts delay and maxWait from 0 to 1e12 seconds, runAt in the years 1 to 9999, and maxAttempts from 1 to the largest int", () => {
    for (const o of [
      { delay: 0 },
      { delay: 1e12 },
      { keyMode: "debounce", delay: 0, maxWait: 0 },
      { keyMode: "debounce", delay: 1e12, maxWait: 1e12 },
      { runAt: new Date("0001-01-01T00:00:00.000Z") },
      { runAt: new Date("9999-12-31T23:59:59.999Z") },
      { maxAttempts: 1 },
      { maxAttempts: 2 ** 31 - 1 },
    ] as const) {
      expect(() => toEnqueueParams("t", null, o)).not.toThrow()
    }
  })

  test("rejects delay, maxWait, runAt, and maxAttempts outside their ranges", () => {
    for (const [o, message] of [
      [{ delay: -1 }, "delay must be a number of seconds from 0 to 1e12, got -1"],
      [{ delay: 1e12 + 1 }, "delay must be"],
      [{ delay: Number.NaN }, "delay must be"],
      [{ keyMode: "debounce", delay: -1, maxWait: 10 }, "delay must be"],
      [{ keyMode: "debounce", delay: 2, maxWait: -1 }, "maxWait must be"],
      [{ keyMode: "debounce", delay: 2, maxWait: Number.POSITIVE_INFINITY }, "maxWait must be"],
      [
        { runAt: new Date(Number.NaN) },
        "runAt must be a valid Date in the years 1 to 9999, got Invalid Date",
      ],
      [{ runAt: new Date("0000-12-31T23:59:59.999Z") }, "runAt must be"],
      [{ runAt: new Date("+010000-01-01T00:00:00.000Z") }, "runAt must be"],
      // As a JavaScript caller passes it, past the types.
      [{ runAt: "2030-01-01" as unknown as Date }, "runAt must be"],
      [{ maxAttempts: 0 }, "maxAttempts must be an integer from 1 to 2147483647, got 0"],
      [{ maxAttempts: 2.5 }, "maxAttempts must be"],
      [{ maxAttempts: 2 ** 31 }, "maxAttempts must be"],
    ] as const) {
      expect(() => toEnqueueParams("t", null, o)).toThrow(RangeError)
      expect(() => toEnqueueParams("t", null, o)).toThrow(message)
    }
  })
})
