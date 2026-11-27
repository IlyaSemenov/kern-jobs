import { describe, expect, test } from "bun:test"

import { defineSchedule } from "../schedule/schedule"

import { defineManifest, findUnimplementedTasks, mergeManifests, toManifestTasks } from "./manifest"
import { defineTask } from "./task"

const indexProduct = defineTask("search.indexProduct").withPayload<{ productId: string }>()
const purgeExpiredSessions = defineTask("sessions.purgeExpired")

/** Schedules built by hand whose scopes, or the scoped batch task, `defineSchedule` would reject, with the error of each. */
const invalidScopeSchedules = (() => {
  const recalculate = defineTask("metrics.recalculate", { scoped: true })
  const scoped = defineSchedule(recalculate, { cron: { minute: 0 }, scopes: ["a"] })
  const unscoped = defineSchedule(purgeExpiredSessions, { cron: { minute: 0 } })
  const digest = defineTask("digest", { scoped: true, key: true }).withBatchPayload<string>({
    maxItems: 10,
  })
  return [
    [
      { ...scoped, scopes: undefined },
      new TypeError(
        `Schedule ${scoped.id} of scoped task ${recalculate.name} must give its scopes`,
      ),
    ],
    [
      { ...unscoped, scopes: () => ["a"] },
      new TypeError(
        `Schedule ${unscoped.id} of task ${purgeExpiredSessions.name}, which is not scoped, takes no scopes`,
      ),
    ],
    [
      { ...scoped, scopes: "a" },
      new TypeError(
        `scopes of schedule ${scoped.id} must be an array of scopes or a function that returns one`,
      ),
    ],
    [{ ...scoped, scopes: ["a", ""] }, RangeError],
    [
      { ...scoped, task: digest, payload: ["tick"] },
      new TypeError("Cannot schedule scoped batch task digest"),
    ],
  ] as const
})()

describe("defineManifest", () => {
  test("defines a manifest of implementations with distinct task names", () => {
    const index = indexProduct.implement(() => {})
    const purge = purgeExpiredSessions.implement(() => {})
    const manifest = defineManifest({ tasks: [index, purge] })
    expect(manifest.tasks).toEqual([index, purge])
    expect(toManifestTasks(manifest.tasks).map(task => [task.name, task.implementation])).toEqual([
      ["search.indexProduct", index],
      ["sessions.purgeExpired", purge],
    ])
  })

  test("rejects several implementations of a task, also the same one listed twice", () => {
    const purge = purgeExpiredSessions.implement(() => {})
    const tasks = [indexProduct.implement(() => {}), purge, indexProduct.implement(() => {}), purge]
    expect(() => defineManifest({ tasks })).toThrow(
      "A manifest takes one implementation of each task; duplicate tasks: search.indexProduct, sessions.purgeExpired",
    )
  })

  test("keeps the failure task of an implementation within it, and takes a failure implementation listed on its own", () => {
    const index = indexProduct.implement(() => {}, { onFailure: () => {} })
    expect(defineManifest({ tasks: [index] }).tasks).toEqual([index])
    expect(defineManifest({ tasks: [index.failure] }).tasks).toEqual([index.failure])
    // An implementation without onFailure beside the failure implementation of another one of its task, such as of an earlier release.
    const plain = indexProduct.implement(() => {})
    expect(defineManifest({ tasks: [plain, index.failure] }).tasks).toEqual([plain, index.failure])
  })

  test("rejects another implementation of the name of the failure task of an implementation", () => {
    const index = indexProduct.implement(() => {}, { onFailure: () => {} })
    const other = indexProduct.implement(() => {}, { onFailure: () => {} })
    for (const tasks of [
      [index, index.failure],
      [other.failure, index],
    ]) {
      expect(() => defineManifest({ tasks })).toThrow(
        "A manifest takes one implementation of each task; duplicate tasks: search.indexProduct:failure",
      )
    }
  })

  test("rejects a failure task whose scoped differs from that of its task", () => {
    const scoped = defineTask("search.indexProduct", { scoped: true })
    const implementation = scoped.implement(() => {}, { onFailure: () => {} })
    const { failure } = implementation
    const unscoped = { ...failure, task: { ...failure.task, scoped: false } }
    expect(() => defineManifest({ tasks: [{ ...implementation, failure: unscoped }] })).toThrow(
      new TypeError(
        "scoped of failure task search.indexProduct:failure must be that of task search.indexProduct",
      ),
    )
  })

  test("rejects a failure of an implementation that is not the implementation of its failure task", () => {
    const { failure } = purgeExpiredSessions.implement(() => {}, { onFailure: () => {} })
    const index = { ...indexProduct.implement(() => {}), failure }
    expect(() => defineManifest({ tasks: [index as never] })).toThrow(
      "failure of the implementation of task search.indexProduct must be the implementation of task search.indexProduct:failure",
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

  test("checks a copy of the tasks, so that an addition during the definition does not escape the check", () => {
    const index = indexProduct.implement(() => {})
    const tasks = [index]
    const manifest = defineManifest({
      tasks,
      get schedules() {
        tasks.push(index)
        return []
      },
    })
    expect(manifest.tasks).toEqual([index])
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
        new TypeError(
          `enqueue of schedule ${schedule.id} must be an object of options, got urgent`,
        ),
      ],
      [
        { ...schedule, task: { name: "missing" } },
        new TypeError(`Schedule ${schedule.id} must have a task defined with defineTask`),
      ],
      [
        { ...schedule, task: { ...purgeExpiredSessions, name: "t\ud800" } },
        "The name of a task must be",
      ],
      ...invalidScopeSchedules,
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

  test("rejects the name of the failure task of an implementation that another manifest implements", () => {
    const index = indexProduct.implement(() => {}, { onFailure: () => {} })
    const search = defineManifest({ tasks: [index] })
    const failures = defineManifest({ tasks: [index.failure] })
    expect(() => mergeManifests(search, failures)).toThrow(
      "duplicate tasks: search.indexProduct:failure",
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

  test("rejects invalid scopes of a schedule and a schedule of a scoped batch task, as defineManifest does", () => {
    for (const [invalid, error] of invalidScopeSchedules) {
      expect(() => mergeManifests({ tasks: [], schedules: [invalid as never] })).toThrow(error)
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

  test("counts the failure task of an implementation as implemented, also listed on its own", () => {
    const index = indexProduct.implement(() => {}, { onFailure: () => {} })
    const tasks = { indexProduct, indexProductFailure: index.failure.task }
    expect(findUnimplementedTasks(tasks, defineManifest({ tasks: [index] }))).toEqual([])
    expect(findUnimplementedTasks(tasks, defineManifest({ tasks: [index.failure] }))).toEqual([
      "search.indexProduct",
    ])
    expect(
      findUnimplementedTasks(tasks, defineManifest({ tasks: [indexProduct.implement(() => {})] })),
    ).toEqual(["search.indexProduct:failure"])
  })

  test("checks each manifest", () => {
    expect(() => findUnimplementedTasks(tasks, [{ tasks: [indexProduct as never] }])).toThrow(
      TypeError,
    )
  })
})
