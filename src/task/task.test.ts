import { describe, expect, test } from "bun:test"

import { fixedBackoff } from "./backoff"
import { defineTask, resolveFailure } from "./task"

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

  test("reads each setting and option once, also from getters of a class and prototypes, and rejects options that are not an object", () => {
    let reads = 0
    class Options {
      lease = 300
      get queue() {
        reads++
        return "q"
      }
    }
    const task = defineTask("t", new Options())
    expect([task.lease, task.options, reads]).toEqual([300, { queue: "q" }, 1])
    expect(defineTask("t", Object.create({ scoped: true, delay: 5 }))).toMatchObject({
      scoped: true,
      options: { delay: 5 },
    })
    expect({ queue: "mail", ...defineTask("t", { delay: 5 }).options }).toStrictEqual({
      queue: "mail",
      delay: 5,
    })
    expect(() => defineTask("t", null as never)).toThrow(
      new TypeError("The options of defineTask must be an object of options, got null"),
    )
  })

  test("keeps the settings and options through withPayload, withKey, and withOptions, which compute the key and the options from the payload", () => {
    const task = defineTask("search.indexProduct", { lease: 120, scoped: true, delay: 5 })
      .withPayload<{ productId: string }>()
      .withKey(({ productId }) => `product:${productId}`)
      .withOptions(({ productId }) => ({ queue: productId }))
    expect([task.name, task.lease, task.scoped, task.options]).toEqual([
      "search.indexProduct",
      120,
      true,
      { delay: 5 },
    ])
    expect(task.keyFor?.({ productId: "42" })).toBe("product:42")
    expect(task.optionsFor?.({ productId: "42" })).toEqual({ queue: "42" })
  })

  test("makes a batch task by withBatchPayload, with its maxItems, and without withOptions", () => {
    const task = defineTask("posts.publish", { key: true, lease: 60 }).withBatchPayload<number>({
      maxItems: 100,
    })
    expect([task.maxItems, task.lease, task.options]).toEqual([100, 60, { key: true }])
    expect("withOptions" in task).toBe(false)
    expect(defineTask("t").maxItems).toBeUndefined()
    expect(
      defineTask("t").implement(() => {}, { onFailure: () => {} }).failure.task.maxItems,
    ).toBeUndefined()
  })

  test("withBatchPayload rejects options that are not an object and a task of keyMode keep with TypeError, and a maxItems that is not a count with RangeError", () => {
    const task = defineTask("t")
    expect(() => task.withBatchPayload(undefined as never)).toThrow(TypeError)
    for (const maxItems of [0, 1.5, 2 ** 31, Number.NaN, "3"]) {
      expect(() => task.withBatchPayload({ maxItems: maxItems as number })).toThrow(
        new RangeError(`maxItems must be an integer from 1 to 2147483647, got ${String(maxItems)}`),
      )
    }
    expect(() =>
      defineTask("keep", { key: true, keyMode: "keep" }).withBatchPayload({ maxItems: 2 }),
    ).toThrow(new TypeError('keyMode "keep" does not apply to batch task keep'))
  })

  test("rejects a name that is not a string of at most 128 bytes in UTF-8", () => {
    // "é" takes 2 bytes.
    expect(defineTask("é".repeat(64)).name).toBe("é".repeat(64))
    expect(() => defineTask("é".repeat(64) + "a")).toThrow(RangeError)
    expect(() => defineTask("é".repeat(64) + "a")).toThrow(
      "The name of a task must be at most 128 bytes in UTF-8",
    )
    // As a JavaScript caller passes it, past the types.
    expect(() => defineTask(5 as unknown as string)).toThrow("The name of a task must be")
  })

  test("rejects a name that is not storable text", () => {
    expect(() => defineTask("t\ud800")).toThrow("The name of a task must be")
    expect(() => defineTask("t\0")).toThrow("The name of a task must be")
  })

  test("rejects a name that ends with :failure, which names the failure task of a task", () => {
    expect(() => defineTask("orders.charge:failure")).toThrow(
      new RangeError(
        "The name of a task must not end with :failure, which names the failure task of a task, got orders.charge:failure",
      ),
    )
    expect(defineTask("orders.charge:failures").name).toBe("orders.charge:failures")
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

  test("rejects a key of withKey that is not a function, of the payload or of an item", () => {
    expect(() => defineTask("t").withKey("k" as never)).toThrow(
      new TypeError("withKey takes a function of the payload"),
    )
    expect(() =>
      defineTask("t")
        .withBatchPayload<string>({ maxItems: 2 })
        .withKey("k" as never),
    ).toThrow(new TypeError("withKey takes a function of an item"))
  })
})

describe("implement", () => {
  test("pairs the task with its handler, before and after withOptions", () => {
    const handler = () => {}
    const plain = defineTask("sessions.purgeExpired")
    expect(plain.implement(handler)).toEqual({ task: plain, handler })
    const task = defineTask("search.indexProduct")
      .withPayload<{ productId: string }>()
      .withKey(({ productId }) => productId)
    const implementation = task.implement(handler)
    expect(implementation.task).toBe(task)
    expect(implementation.handler).toBe(handler)
  })

  test("rejects a handler that is not a function", () => {
    expect(() => defineTask("t").implement(undefined as never)).toThrow(TypeError)
  })

  test("makes of onFailure the implementation of the failure task, of the name of the task with :failure and of its scope setting, and adds no field without it", () => {
    const handler = () => {}
    const onFailure = () => {}
    const task = defineTask("orders.charge", { lease: 60, backoff: () => 1, maxAttempts: 3 })
    const implementation = task.implement(handler, { onFailure })
    expect(implementation).toEqual({
      task,
      handler,
      failure: {
        task: expect.objectContaining({ name: "orders.charge:failure" }),
        handler: onFailure,
      },
    })
    // The failure task takes neither the lease, the backoff, nor the options of an enqueue of the task.
    const { name, lease, backoff, scoped, options } = implementation.failure.task
    expect({ name, lease, backoff, scoped, options }).toEqual({
      name: "orders.charge:failure",
      lease: undefined,
      backoff: undefined,
      scoped: undefined,
      options: {},
    })
    const scopedTask = defineTask("orders.charge", { scoped: true })
    expect(scopedTask.implement(handler, { onFailure }).failure.task.scoped).toBe(true)
    expect(task.implement(handler, {})).toEqual({ task, handler })
    expect(task.implement(handler, { onFailure: undefined })).toEqual({ task, handler })
  })

  test("gives the failure task the maxAttempts and the backoff of an onFailure given as an object", () => {
    const onFailure = () => {}
    const backoff = () => 5
    const task = defineTask("orders.charge", { backoff: () => 1, maxAttempts: 3 })
    const { failure } = task.implement(() => {}, {
      onFailure: { handler: onFailure, maxAttempts: 100, backoff },
    })
    expect(failure.handler).toBe(onFailure)
    expect([failure.task.options, failure.task.backoff, failure.task.lease]).toEqual([
      { maxAttempts: 100 },
      backoff,
      undefined,
    ])
    const plain = task.implement(() => {}, { onFailure: { handler: onFailure } }).failure
    expect([plain.task.options, plain.task.backoff]).toEqual([{}, undefined])
  })

  test("rejects options that are not an object, an onFailure that is neither a function nor an object of a handler, its invalid settings, and an onFailure of a task whose name is too long to take :failure", () => {
    const task = defineTask("orders.charge")
    const implement = (options: unknown) => () => task.implement(() => {}, options as never)
    expect(implement(null)).toThrow(new TypeError("The options of implement must be an object"))
    for (const onFailure of [
      null,
      "orders.charge.failed",
      defineTask("orders.charge.failed"),
      {},
    ]) {
      expect(implement({ onFailure })).toThrow(
        new TypeError(
          "onFailure of task orders.charge must be a handler of the failures of its jobs, or an object with it as handler",
        ),
      )
    }
    const handler = () => {}
    expect(implement({ onFailure: { handler, backoff: 5 } })).toThrow(
      new TypeError("backoff of onFailure of task orders.charge must be a function of the attempt"),
    )
    for (const maxAttempts of [0, 1.5, "3", null]) {
      expect(implement({ onFailure: { handler, maxAttempts } })).toThrow(RangeError)
    }
    const long = defineTask("x".repeat(121))
    expect(long.implement(() => {}).task).toBe(long)
    expect(() => long.implement(() => {}, { onFailure: () => {} })).toThrow(RangeError)
    expect(
      defineTask("x".repeat(120)).implement(() => {}, { onFailure: () => {} }).failure.task.name,
    ).toHaveLength(128)
  })
})

describe("resolveFailure", () => {
  const task = defineTask("orders.charge")

  test("gives the failure task and the default attempts of an enqueue, or the maxAttempts of onFailure, and nothing without a failure implementation", () => {
    const { failure } = task.implement(() => {}, { onFailure: () => {} })
    expect(resolveFailure("orders.charge", failure)).toEqual({
      name: "orders.charge:failure",
      task: failure.task,
      maxAttempts: 25,
    })
    const limited = task.implement(() => {}, { onFailure: { handler: () => {}, maxAttempts: 50 } })
    expect(resolveFailure("orders.charge", limited.failure)?.maxAttempts).toBe(50)
    expect(resolveFailure("orders.charge", undefined)).toBeUndefined()
  })

  test("reads the task of the failure implementation and its name once, and gives the checked name", () => {
    const { failure } = task.implement(() => {}, { onFailure: () => {} })
    const reads = { task: 0, name: 0 }
    const names = ["orders.charge:failure", "orders.refund:failure"]
    const named = {
      ...failure.task,
      get name() {
        reads.name++
        return names.shift()!
      },
    }
    const counted = {
      handler: failure.handler,
      get task() {
        reads.task++
        return named
      },
    }
    const resolved = resolveFailure("orders.charge", counted)
    expect([resolved?.name, resolved?.task]).toEqual(["orders.charge:failure", named])
    expect(reads).toEqual({ task: 1, name: 1 })
  })

  test("rejects a failure that is not an implementation of the failure task of the task, and a maxAttempts that is not a count", () => {
    const error = new TypeError(
      "failure of the implementation of task orders.charge must be the implementation of task orders.charge:failure that implement makes of onFailure",
    )
    const { failure } = task.implement(() => {}, { onFailure: () => {} })
    const other = defineTask("orders.refund").implement(() => {}, { onFailure: () => {} })
    for (const invalid of [null, () => {}, { task: failure.task }, other.failure, other]) {
      expect(() => resolveFailure("orders.charge", invalid)).toThrow(error)
    }
    for (const maxAttempts of [0, null]) {
      const invalid = { ...failure, task: { ...failure.task, options: { maxAttempts } } }
      expect(() => resolveFailure("orders.charge", invalid)).toThrow(RangeError)
    }
  })

  test("rejects a failure implementation of a failure task, which implement rejects too", () => {
    const error = new TypeError("Failure task orders.charge:failure takes no onFailure of its own")
    const { failure } = task.implement(() => {}, { onFailure: () => {} })
    expect(() => failure.task.implement(() => {}, { onFailure: () => {} })).toThrow(error)
    expect(failure.task.implement(failure.handler).task).toBe(failure.task)
    expect(() => resolveFailure("orders.charge:failure", failure)).toThrow(error)
  })
})
