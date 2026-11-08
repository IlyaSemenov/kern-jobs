import { describe, expect, spyOn, test } from "bun:test"

import { defaultBackoff, exponentialBackoff, fixedBackoff } from "./backoff"

const delays = (backoff: (attempt: number) => number, attempts: number[]) => attempts.map(backoff)

describe("exponentialBackoff", () => {
  test("grows by the factor from the base after each attempt, up to max", () => {
    expect(delays(exponentialBackoff({ base: 10, factor: 3, max: 100 }), [1, 2, 3, 4])).toEqual([
      10, 30, 90, 100,
    ])
    expect(delays(exponentialBackoff({ base: 5, factor: 1 }), [1, 10])).toEqual([5, 5])
  })

  test("takes the defaults for omitted options and options given as undefined", () => {
    const attempts = [1, 2, 12, 13]
    expect(delays(exponentialBackoff({ base: undefined, max: undefined }), attempts)).toEqual(
      delays(defaultBackoff, attempts),
    )
  })

  test("shortens each delay by a random share up to jitter, also at max", () => {
    const backoff = exponentialBackoff({ base: 10, max: 10, jitter: 0.2 })
    const random = spyOn(Math, "random")
    try {
      random.mockReturnValue(0)
      expect(backoff(1)).toBe(10)
      random.mockReturnValue(0.5)
      expect(backoff(1)).toBe(9)
      expect(backoff(5)).toBe(9)
    } finally {
      random.mockRestore()
    }
  })

  test("rejects options outside their ranges", () => {
    const invalid = [
      { base: 0 },
      { base: -1 },
      { base: Infinity },
      { base: 1e20 },
      { max: -1 },
      { max: NaN },
      { max: Infinity },
      { max: 1e20 },
      { factor: 0.5 },
      { factor: NaN },
      { factor: Infinity },
      { jitter: -0.1 },
      { jitter: 1.1 },
      { jitter: NaN },
      // As a JavaScript caller passes it, past the types.
      { jitter: "0.2" as unknown as number },
    ]
    for (const options of invalid) {
      expect(() => exponentialBackoff(options), JSON.stringify(options)).toThrow(RangeError)
    }
  })
})

describe("defaultBackoff", () => {
  test("waits 2 seconds after the first attempt, doubled after each next one, up to an hour", () => {
    expect(delays(defaultBackoff, [1, 2, 3, 11, 12, 13, 10_000])).toEqual([
      2, 4, 8, 2048, 3600, 3600, 3600,
    ])
  })
})

describe("fixedBackoff", () => {
  test("waits the same delay after every attempt", () => {
    expect(delays(fixedBackoff(30), [1, 2, 100])).toEqual([30, 30, 30])
    expect(fixedBackoff(0)(1)).toBe(0)
  })

  test("rejects a delay that is not a number of seconds from 0 to 1e12", () => {
    for (const seconds of [-1, NaN, Infinity, 1e20]) {
      expect(() => fixedBackoff(seconds)).toThrow(RangeError)
    }
  })
})
