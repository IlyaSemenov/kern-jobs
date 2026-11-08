import { describe, expect, test } from "bun:test"

import { fixedBackoff } from "./backoff"
import { defineTask } from "./task"

describe("defineTask", () => {
  test("defines a task by its name, with its settings and the options of each enqueue", () => {
    const plain = defineTask("sessions.purgeExpired")
    expect([plain.name, plain.lease, plain.backoff, plain.scoped, plain.options]).toEqual([
      "sessions.purgeExpired",
      undefined,
      undefined,
      undefined,
      {},
    ])

    const backoff = fixedBackoff(30)
    const task = defineTask("accounts.sync", {
      lease: 300,
      backoff,
      scoped: true,
      delay: 5,
      maxAttempts: 3,
    })
    expect([task.lease, task.backoff, task.scoped, task.options]).toEqual([
      300,
      backoff,
      true,
      { delay: 5, maxAttempts: 3 },
    ])
  })

  test("keeps the settings and options through withPayload and withOptions, which computes options from the payload", () => {
    const task = defineTask("search.indexProduct", { lease: 120, scoped: true, delay: 5 })
      .withPayload<{ productId: string }>()
      .withOptions(({ productId }) => ({ key: `product:${productId}` }))
    expect([task.name, task.lease, task.scoped, task.options]).toEqual([
      "search.indexProduct",
      120,
      true,
      { delay: 5 },
    ])
    expect(task.optionsFor?.({ productId: "42" })).toEqual({ key: "product:42" })
  })

  test("rejects a name that is not a string of at most 128 bytes in UTF-8", () => {
    // "é" takes 2 bytes.
    expect(defineTask("é".repeat(64)).name).toBe("é".repeat(64))
    expect(() => defineTask("é".repeat(64) + "a")).toThrow(RangeError)
    expect(() => defineTask("é".repeat(64) + "a")).toThrow(
      "The name of a task must be at most 128 bytes in UTF-8",
    )
    // As a JavaScript caller passes it, past the types.
    expect(() => defineTask(5 as unknown as string)).toThrow(
      new TypeError("The name of a task must be a string"),
    )
  })

  test("rejects a lease that is not a number of seconds above 0 up to 1e12, a backoff that is not a function, and a scoped that is not a boolean", () => {
    for (const lease of [0, -1, NaN, Infinity, "60", 1e12 + 1]) {
      expect(() => defineTask("t", { lease: lease as number })).toThrow(RangeError)
    }
    expect(() => defineTask("t", { backoff: 5 as never })).toThrow(TypeError)
    expect(() => defineTask("t", { scoped: "yes" as never })).toThrow(
      new TypeError("scoped must be a boolean"),
    )
  })

  test("rejects options of withOptions that are not a function of the payload", () => {
    expect(() => defineTask("t").withOptions({ delay: 5 } as never)).toThrow(
      "withOptions takes a function of the payload; pass static options to defineTask",
    )
  })
})

describe("implement", () => {
  test("pairs the task with its handler, before and after withOptions", () => {
    const handler = () => {}
    const plain = defineTask("sessions.purgeExpired")
    expect(plain.implement(handler)).toEqual({ task: plain, handler })
    const task = defineTask("search.indexProduct")
      .withPayload<{ productId: string }>()
      .withOptions(({ productId }) => ({ key: productId }))
    const implementation = task.implement(handler)
    expect(implementation.task).toBe(task)
    expect(implementation.handler).toBe(handler)
  })

  test("rejects a handler that is not a function", () => {
    expect(() => defineTask("t").implement(undefined as never)).toThrow(TypeError)
  })
})
