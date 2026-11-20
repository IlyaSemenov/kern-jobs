import { describe, expect, test } from "bun:test"

import { defineSchedule } from "../schedule/schedule"

import { defineManifest, findUnimplementedTasks, mergeManifests, toTaskMap } from "./manifest"
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

  test("accepts a schedule by its shape and rejects duplicate schedule IDs", () => {
    const schedule = defineSchedule(purgeExpiredSessions, { cron: { minute: 0 } })
    const copy = { ...schedule }
    expect(defineManifest({ tasks: [], schedules: [copy] }).schedules).toEqual([copy])
    expect(() => defineManifest({ tasks: [], schedules: [schedule, copy] })).toThrow(
      `Duplicate schedule ID in a manifest: ${schedule.id}`,
    )
  })

  test("checks a schedule built by hand as defineSchedule does", () => {
    const schedule = defineSchedule(purgeExpiredSessions, { cron: { minute: 0 } })
    const { enabled: _, ...withoutEnabled } = schedule
    for (const [invalid, error] of [
      [withoutEnabled, new TypeError(`enabled of schedule ${schedule.id} must be a boolean`)],
      [{ ...schedule, cron: { minute: 60 } }, new RangeError("Invalid minute: 60")],
      [
        { ...schedule, catchUp: {} },
        new RangeError(
          `catchUp.within of schedule ${schedule.id} must be a positive whole number of seconds`,
        ),
      ],
      [
        { ...schedule, task: purgeExpiredSessions.implement(() => {}) },
        new TypeError(`Schedule ${schedule.id} must have a task defined with defineTask`),
      ],
      [
        { ...schedule, enqueue: "urgent" },
        new TypeError(`enqueue of schedule ${schedule.id} must be an object of enqueue options`),
      ],
      [
        { ...schedule, task: { name: "missing" } },
        new TypeError(`Schedule ${schedule.id} must have a task defined with defineTask`),
      ],
      [
        { ...schedule, task: { ...purgeExpiredSessions, name: "t\ud800" } },
        "The name of a task must be",
      ],
    ] as const) {
      expect(() => defineManifest({ tasks: [], schedules: [invalid as never] })).toThrow(error)
    }
    // A hole of a sparse array is a missing schedule, which `map` would skip.
    const sparseSchedules: never[] = []
    sparseSchedules.length = 1
    expect(() => defineManifest({ tasks: [], schedules: sparseSchedules })).toThrow(
      new TypeError("A manifest takes schedules defined with defineSchedule"),
    )
  })
})

describe("mergeManifests", () => {
  const index = indexProduct.implement(() => {})
  const purge = purgeExpiredSessions.implement(() => {})
  const nightly = defineSchedule(indexProduct, {
    id: "search.nightly",
    payload: { productId: "all" },
    cron: { hour: 0, minute: 0 },
  })
  const hourly = defineSchedule(purgeExpiredSessions, { cron: { minute: 0 } })

  test("keeps the order of the manifests and freezes the manifest and both lists", () => {
    const search = defineManifest({ tasks: [index], schedules: [nightly] })
    const sessions = defineManifest({ tasks: [purge], schedules: [hourly] })
    const manifest = mergeManifests(search, defineManifest({ tasks: [] }), sessions)
    expect(manifest).toEqual({ tasks: [index, purge], schedules: [nightly, hourly] })
    expect(Object.isFrozen(manifest)).toBe(true)
    expect(Object.isFrozen(manifest.tasks)).toBe(true)
    expect(Object.isFrozen(manifest.schedules)).toBe(true)
    expect(mergeManifests()).toEqual({ tasks: [], schedules: [] })
  })

  test("rejects a task name or schedule ID that two manifests share, also of one manifest given twice", () => {
    const sessions = defineManifest({ tasks: [purge], schedules: [hourly] })
    expect(() =>
      mergeManifests(sessions, { tasks: [purgeExpiredSessions.implement(() => {})] }),
    ).toThrow("duplicate tasks: sessions.purgeExpired")
    expect(() => mergeManifests(sessions, { tasks: [], schedules: [hourly] })).toThrow(
      `Duplicate schedule ID in a manifest: ${hourly.id}`,
    )
    expect(() => mergeManifests(sessions, sessions)).toThrow(
      "duplicate tasks: sessions.purgeExpired",
    )
  })

  test("rejects a malformed manifest, as defineManifest does", () => {
    // As a JavaScript caller passes them, past the types: also the values that flattening would accept, a single task or schedule in place of its array, and a sparse array.
    const sparse: never[] = []
    sparse.length = 1
    for (const manifest of [
      undefined,
      {},
      { tasks: "search.indexProduct" },
      { tasks: index },
      { tasks: sparse },
      { tasks: [index], schedules: "nightly" },
      { tasks: [index], schedules: nightly },
      { tasks: [index], schedules: null },
      { tasks: [index], schedules: sparse },
    ]) {
      expect(() => mergeManifests(manifest as never)).toThrow(TypeError)
    }
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
