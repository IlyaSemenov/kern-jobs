import { describe, expect, test } from "bun:test"

import { defineManifest, findUnimplementedTasks, toTaskMap } from "./manifest"
import { defineTask, type TaskImplementation } from "./task"

const indexProduct = defineTask("search.indexProduct").withPayload<{ productId: string }>()
const purgeExpiredSessions = defineTask("sessions.purgeExpired")

describe("defineManifest", () => {
  test("defines a manifest of implementations with distinct task names", () => {
    const index = indexProduct.implement(() => {})
    const purge = purgeExpiredSessions.implement(() => {})
    const manifest = defineManifest({ tasks: [index, purge] })
    expect(manifest.tasks).toEqual([index, purge])
    expect(toTaskMap(manifest.tasks)).toEqual(
      new Map<string, TaskImplementation<string, unknown>>([
        ["search.indexProduct", index],
        ["sessions.purgeExpired", purge],
      ]),
    )
  })

  test("rejects several implementations of a task, also the same one listed twice", () => {
    const purge = purgeExpiredSessions.implement(() => {})
    const tasks = [indexProduct.implement(() => {}), purge, indexProduct.implement(() => {}), purge]
    expect(() => defineManifest({ tasks })).toThrow(
      "A manifest takes one implementation of each task; duplicate tasks: search.indexProduct, sessions.purgeExpired",
    )
  })

  test("rejects an element that is not an implementation", () => {
    // As a JavaScript caller passes them, past the types: a task without implement, and nothing.
    for (const element of [indexProduct, undefined, { task: indexProduct }]) {
      expect(() => defineManifest({ tasks: [element as never] })).toThrow(TypeError)
    }
  })

  test("rejects tasks that are not an array", () => {
    for (const manifest of [undefined, {}, { tasks: "search.indexProduct" }]) {
      expect(() => defineManifest(manifest as never)).toThrow(
        new TypeError("manifest.tasks must be an array of task implementations"),
      )
    }
  })

  test("keeps its implementations when the array given to it changes later", () => {
    const tasks = [indexProduct.implement(() => {})]
    const manifest = defineManifest({ tasks })
    tasks.push(indexProduct.implement(() => {}))
    expect(manifest.tasks).toHaveLength(1)
    expect(Object.isFrozen(manifest.tasks)).toBe(true)
  })
})

describe("findUnimplementedTasks", () => {
  const syncAccount = defineTask("accounts.sync").withPayload<{ accountId: string }>()
  const tasks = { indexProduct, purgeExpiredSessions, syncAccount }

  test("returns the names of the tasks that no manifest implements", () => {
    const search = defineManifest({ tasks: [indexProduct.implement(() => {})] })
    const accounts = defineManifest({ tasks: [syncAccount.implement(() => {})] })
    expect(findUnimplementedTasks(tasks, search)).toEqual([
      "sessions.purgeExpired",
      "accounts.sync",
    ])
    expect(findUnimplementedTasks(tasks, [search, accounts])).toEqual(["sessions.purgeExpired"])
    expect(findUnimplementedTasks({ indexProduct, syncAccount }, [search, accounts])).toEqual([])
  })

  test("skips the values that are not tasks and names a task exported twice once", () => {
    const exports = {
      indexProduct,
      alias: indexProduct,
      defineCleanup: (name: string) => defineTask(name),
      schedule: { name: "nightly" },
      version: 1,
      nothing: null,
    }
    expect(findUnimplementedTasks(exports, defineManifest({ tasks: [] }))).toEqual([
      "search.indexProduct",
    ])
  })

  test("checks each manifest", () => {
    expect(() => findUnimplementedTasks(tasks, [{ tasks: [indexProduct as never] }])).toThrow(
      TypeError,
    )
  })
})
