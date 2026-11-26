// The test double of the jobs API: recorded enqueues and their options, its pending jobs, captures, the queries of a capture by task and scope, and the runs of a handler and of onFailure in its scope, also through runHandler.

import { describe, expect, mock, test } from "bun:test"

import { ALL_SCOPES } from "../producer/jobs"
import type { JobFailure } from "../task/failure"
import type { RunHandler } from "../task/run-handler"
import { defineTask, type HandlerContext } from "../task/task"

import { createTestJobs } from "./test-jobs"

const sendEmailTask = defineTask("email.send").withPayload<{ to: string }>()
const indexProductTask = defineTask("search.indexProduct", {
  key: "index",
  queue: "search",
}).withPayload<{ productId: string }>()
const reportTask = defineTask("reports.build", { scoped: true }).withPayload<{ id: number }>()

describe("enqueue", () => {
  test("records the task, the scope, the payload as passed, and the options of defineTask, withKey, withOptions, and the overrides as an enqueue merges them", async () => {
    const date = new Date("2030-01-01T00:00:00Z")
    const optionsFor = mock((_payload: { at: Date }) => ({ delay: 5 }))
    const task = defineTask("t", { maxAttempts: 3, queue: "q" })
      .withPayload<{ at: Date }>()
      .withKey(payload => `k:${payload.at.getTime()}`)
      .withOptions(optionsFor)
    const jobs = createTestJobs()
    await jobs.enqueue(task, { at: date }, { runAt: date, queue: null })
    expect(optionsFor).toHaveBeenCalledTimes(1)
    expect(jobs.calls).toStrictEqual([
      {
        task: "t",
        scope: null,
        payload: { at: date },
        options: { maxAttempts: 3, queue: null, key: `k:${date.getTime()}`, runAt: date },
      },
    ])
  })

  test("takes a key of the overrides from a getter of a class or a prototype, which replaces the key of withKey", async () => {
    const keyFor = mock((payload: { id: number }) => `id:${payload.id}`)
    const task = defineTask("t", { key: "static" }).withPayload<{ id: number }>().withKey(keyFor)
    class Overrides {
      get key() {
        return "getter"
      }
    }
    const jobs = createTestJobs()
    await jobs.enqueue(task, { id: 1 }, new Overrides())
    await jobs.enqueue(task, { id: 2 }, Object.create({ key: "inherited" }))
    expect(keyFor).not.toHaveBeenCalled()
    expect(jobs.calls.map(call => call.options.key)).toEqual(["getter", "inherited"])
  })

  test("reads each field of the overrides and of the options of withOptions once, also from getters of a class and prototypes", async () => {
    const reads: string[] = []
    class Options {
      get queue() {
        reads.push("queue")
        return `q${reads.length}`
      }
    }
    class Overrides {
      get delay() {
        reads.push("delay")
        return reads.length
      }
    }
    const task = defineTask("t", { runAt: new Date("2030-01-01T00:00:00Z") })
      .withPayload<number>()
      .withOptions(() => new Options())
    const jobs = createTestJobs()
    await jobs.enqueue(task, 1, new Overrides())
    await jobs.enqueue(task, 2, Object.create({ delay: 5, maxAttempts: 2 }))
    expect(reads).toEqual(["delay", "queue", "queue"])
    expect(jobs.calls.map(call => call.options)).toStrictEqual([
      { queue: "q2", delay: 1 },
      { queue: "q3", delay: 5, maxAttempts: 2 },
    ])
  })

  test("rejects overrides and options of withOptions that are not an object with TypeError, and records nothing", async () => {
    const task = defineTask("t")
      .withPayload<number>()
      .withOptions(() => 5 as never)
    const jobs = createTestJobs()
    await expect(jobs.enqueue(sendEmailTask, { to: "a" }, null as never)).rejects.toThrow(
      new TypeError("The overrides of an enqueue must be an object of options, got null"),
    )
    await expect(jobs.enqueue(task, 1)).rejects.toThrow(
      new TypeError("withOptions of task t must be an object of options, got 5"),
    )
    expect(jobs.calls).toEqual([])
  })

  test("records only the options that a layer gave, also an explicit keyMode replace", async () => {
    const jobs = createTestJobs()
    await jobs.enqueue(sendEmailTask, { to: "a" })
    await jobs.enqueue(sendEmailTask, { to: "b" }, { keyMode: "replace" })
    expect(jobs.calls.map(call => call.options)).toStrictEqual([{}, { keyMode: "replace" }])
  })

  test("drops maxWait outside debounce and resetAttempts with keep, which the enqueue ignores", async () => {
    const task = defineTask("t", {
      key: "k",
      keyMode: "debounce",
      delay: 2,
      maxWait: 10,
      resetAttempts: true,
    })
    const jobs = createTestJobs()
    await jobs.enqueue(task)
    await jobs.enqueue(task, undefined, { keyMode: "replace" })
    await jobs.enqueue(task, undefined, { keyMode: "keep" })
    expect(jobs.calls.map(call => call.options)).toStrictEqual([
      { key: "k", keyMode: "debounce", delay: 2, maxWait: 10, resetAttempts: true },
      { key: "k", keyMode: "replace", delay: 2, resetAttempts: true },
      { key: "k", keyMode: "keep", delay: 2 },
    ])
  })

  test("rejects invalid options and a payload that JSON cannot serialize as createJobs does, and records nothing", async () => {
    const jobs = createTestJobs()
    const date = new Date()
    await expect(
      jobs.enqueue(sendEmailTask, { to: "a" }, { delay: 1, runAt: date } as never),
    ).rejects.toThrow(TypeError)
    await expect(jobs.enqueue(sendEmailTask, { to: "a" }, { delay: -1 })).rejects.toThrow(
      RangeError,
    )
    await expect(jobs.enqueue(sendEmailTask, { to: 1n } as never)).rejects.toThrow(TypeError)
    expect(jobs.calls).toEqual([])
  })

  test("resolves to the id of the pending job of the key of its task and scope and created false, in every key mode, and to a new id otherwise", async () => {
    const jobs = createTestJobs()
    expect(await jobs.enqueue(indexProductTask, { productId: "1" })).toEqual({
      id: "1",
      created: true,
    })
    for (const overrides of [
      { keyMode: "replace" },
      { keyMode: "keep" },
      { keyMode: "debounce", delay: 1, maxWait: 5 },
    ] as const) {
      expect(await jobs.enqueue(indexProductTask, { productId: "2" }, overrides)).toEqual({
        id: "1",
        created: false,
      })
    }
    expect(await jobs.enqueue(indexProductTask, { productId: "3" }, { key: "other" })).toEqual({
      id: "2",
      created: true,
    })
    expect(await jobs.enqueue(sendEmailTask, { to: "a" }, { key: "index" })).toEqual({
      id: "3",
      created: true,
    })
    expect(await jobs.enqueue(sendEmailTask, { to: "a" })).toEqual({ id: "4", created: true })
    expect(await jobs.enqueue(sendEmailTask, { to: "a" })).toEqual({ id: "5", created: true })
  })

  test("keeps a key per scope", async () => {
    let scope = "a"
    const task = defineTask("t", { scoped: true, key: "k" })
    const jobs = createTestJobs({ scope: () => scope })
    expect(await jobs.enqueue(task)).toEqual({ id: "1", created: true })
    scope = "b"
    expect(await jobs.enqueue(task)).toEqual({ id: "2", created: true })
    expect(jobs.calls.map(call => call.scope)).toEqual(["a", "b"])
  })

  test("rejects an enqueue of a scoped task without a scope or with ALL_SCOPES, and createTestJobs rejects an invalid scope, as createJobs does", async () => {
    await expect(createTestJobs().enqueue(reportTask, { id: 1 })).rejects.toThrow(TypeError)
    await expect(
      createTestJobs({ scope: ALL_SCOPES }).enqueue(reportTask, { id: 1 }),
    ).rejects.toThrow(TypeError)
    expect(() => createTestJobs({ scope: "" })).toThrow(RangeError)
    expect(() => createTestJobs({ scope: 1 as never })).toThrow(TypeError)
  })
})

describe("enqueueMany", () => {
  test("records an enqueue of each payload in their order, and resolves to the id of the pending job of a key and to new ids in that order", async () => {
    const task = defineTask("t")
      .withPayload<string | null>()
      .withKey(key => key)
    const jobs = createTestJobs()
    const existing = await jobs.enqueue(task, "a")
    const captured = await jobs.capture(() => jobs.enqueueMany(task, [null, "a", "b"]))
    expect(captured.result).toEqual([
      { id: "2", created: true },
      { id: existing.id, created: false },
      { id: "3", created: true },
    ])
    expect(captured.calls.map(call => call.payload)).toEqual([null, "a", "b"])
    expect(jobs.calls.map(call => call.options.key)).toEqual(["a", null, "a", "b"])
  })

  test("reads the overrides once per call, also without payloads", async () => {
    const task = defineTask("t").withPayload<string>()
    let reads = 0
    const overrides = {
      get queue() {
        reads++
        return `q${reads}`
      },
    }
    const jobs = createTestJobs()
    await jobs.enqueueMany(task, ["a", "b"], overrides)
    await jobs.enqueueMany(task, [], overrides)
    expect(reads).toBe(2)
    expect(jobs.calls.map(call => call.options.queue)).toEqual(["q1", "q1"])
  })

  test("rejects two payloads of one key and a payload that JSON cannot serialize as createJobs does, and records nothing, also of the payloads before it", async () => {
    const task = defineTask("t").withPayload<string | bigint>()
    const jobs = createTestJobs()
    const captured = await jobs.capture(async () => {
      await expect(jobs.enqueueMany(task, ["a", "a"], { key: true })).rejects.toThrow(
        new RangeError('enqueueMany takes each key once, got "" twice'),
      )
      await expect(jobs.enqueueMany(task, ["a", 1n])).rejects.toThrow(TypeError)
    })
    expect([captured.calls, jobs.calls]).toEqual([[], []])
    expect(await jobs.enqueue(task, "a", { key: true })).toEqual({ id: "1", created: true })
  })
})

describe("enqueueItems", () => {
  const publishPosts = defineTask("posts.publish", { key: true }).withBatchPayload<number>({
    maxItems: 3,
  })

  test("records one enqueue of the items, also of none, and resolves to the jobs that they fill one enqueue after another, in the order of the items", async () => {
    const jobs = createTestJobs()
    expect(await jobs.enqueueItems(publishPosts, [1, 2])).toEqual([{ id: "1", created: true }])
    expect(await jobs.enqueueItems(publishPosts, [3, 4, 5, 6, 7])).toEqual([
      { id: "1", created: false },
      { id: "2", created: true },
      { id: "3", created: true },
    ])
    expect(await jobs.enqueueItems(publishPosts, [])).toEqual([])
    expect(await jobs.enqueueItems(publishPosts, [8])).toEqual([{ id: "3", created: false }])
    expect(jobs.calls.map(call => call.payload)).toEqual([[1, 2], [3, 4, 5, 6, 7], [], [8]])
  })

  test("records the key that withKey gives the items, and rejects items of several keys, recording nothing", async () => {
    const keyed = defineTask("posts.publish")
      .withBatchPayload<{ siteId: string; postId: number }>({ maxItems: 3 })
      .withKey(post => `site:${post.siteId}`)
    const jobs = createTestJobs()
    await jobs.enqueueItems(keyed, [{ siteId: "a", postId: 1 }])
    await expect(
      jobs.enqueueItems(keyed, [
        { siteId: "a", postId: 2 },
        { siteId: "b", postId: 3 },
      ]),
    ).rejects.toThrow(TypeError)
    expect(jobs.calls.map(call => call.options.key)).toEqual(["site:a"])
  })

  test("uses a key of the overrides from a getter in place of withKey, and rejects an inherited or misspelled override other than the key", async () => {
    const keyFor = mock((post: { siteId: string }) => `site:${post.siteId}`)
    const keyed = defineTask("posts.publish")
      .withBatchPayload<{ siteId: string }>({ maxItems: 3 })
      .withKey(keyFor)
    class Overrides {
      get key() {
        return "all"
      }
    }
    const jobs = createTestJobs()
    await jobs.enqueueItems(keyed, [{ siteId: "a" }, { siteId: "b" }], new Overrides())
    await expect(
      jobs.enqueueItems(keyed, [{ siteId: "a" }], Object.create({ queue: "q" })),
    ).rejects.toThrow(
      new TypeError(
        "An enqueue of batch task posts.publish takes only a key, got queue; its other options come from defineTask",
      ),
    )
    await expect(jobs.enqueueItems(publishPosts, [1], { kye: "site:a" } as never)).rejects.toThrow(
      new TypeError(
        "An enqueue of batch task posts.publish takes only a key, got kye; its other options come from defineTask",
      ),
    )
    expect(keyFor).not.toHaveBeenCalled()
    expect(jobs.calls.map(call => call.options.key)).toEqual(["all"])
  })

  test("seals the pending job of the key with another maxItems and appends to new jobs", async () => {
    const jobs = createTestJobs()
    await jobs.enqueueItems(publishPosts, [1, 2])
    const resized = defineTask("posts.publish", { key: true }).withBatchPayload<number>({
      maxItems: 2,
    })
    expect(await jobs.enqueueItems(resized, [3, 4, 5])).toEqual([
      { id: "2", created: true },
      { id: "3", created: true },
    ])
    expect(await jobs.enqueueItems(publishPosts, [6])).toEqual([{ id: "4", created: true }])
  })

  test("rejects a merge of the two kinds with the message and code of the database, and records and adds nothing, also for a late conflict in enqueueMany", async () => {
    const ordinary = defineTask("posts.publish")
      .withPayload<number>()
      .withKey(n => `k\\${n}`)
    const jobs = createTestJobs()
    await jobs.enqueue(ordinary, 1)
    await expect(jobs.enqueueItems(publishPosts, [2], { key: "k\\1" })).rejects.toMatchObject({
      message:
        "The pending job of key E'k\\\\1' of task posts.publish is not a batch, so items cannot append to it",
      code: "55000",
    })
    await jobs.enqueueItems(publishPosts, [3], { key: "k\\3" })
    await expect(jobs.enqueueMany(ordinary, [1, 2, 3])).rejects.toMatchObject({
      message:
        "The pending job of key E'k\\\\3' of task posts.publish is a batch, so a job that is not a batch cannot merge into it",
      code: "55000",
    })
    expect(jobs.calls.map(call => call.payload)).toEqual([1, [3]])
    expect(await jobs.enqueue(ordinary, 2)).toEqual({ id: "3", created: true })
  })
})

describe("administration", () => {
  test("delete deletes a pending job by its id once, and an enqueue of its key then adds a new one", async () => {
    const jobs = createTestJobs({ scope: "a" })
    const { id } = await jobs.enqueue(reportTask, { id: 1 })
    expect(await jobs.delete("99")).toBe(false)
    expect(await jobs.delete(id)).toBe(true)
    expect(await jobs.delete(id)).toBe(false)
    const keyed = await jobs.enqueue(reportTask, { id: 1 }, { key: "k" })
    expect(await jobs.delete(keyed.id)).toBe(true)
    expect(await jobs.enqueue(reportTask, { id: 1 }, { key: "k" })).toMatchObject({ created: true })
  })

  test("delete of another scope does not reach the job, and ALL_SCOPES of another instance does not either", async () => {
    let scope = "a"
    const jobs = createTestJobs({ scope: () => scope })
    const { id } = await jobs.enqueue(reportTask, { id: 1 })
    scope = "b"
    expect(await jobs.delete(id)).toBe(false)
    expect(await createTestJobs({ scope: ALL_SCOPES }).delete(id)).toBe(false)
    scope = "a"
    expect(await jobs.delete(id)).toBe(true)
  })

  test("deletePending deletes the pending jobs of the scope, of a task, or of a key, as createJobs does", async () => {
    const jobs = createTestJobs({ scope: "a" })
    await jobs.enqueue(reportTask, { id: 1 }, { key: "1" })
    await jobs.enqueue(reportTask, { id: 2 }, { key: "2" })
    await jobs.enqueue(sendEmailTask, { to: "a" })
    await jobs.enqueue(indexProductTask, { productId: "1" })
    expect(await jobs.deletePending({ task: reportTask, key: "1" })).toBe(1)
    expect(await jobs.deletePending({ task: reportTask })).toBe(1)
    // Without a task, only the jobs of the scope "a", none of which remain.
    expect(await jobs.deletePending()).toBe(0)
    expect(await createTestJobs().deletePending()).toBe(0)
    expect(await jobs.deletePending({ task: sendEmailTask })).toBe(1)
    expect(await jobs.deletePending({ task: indexProductTask, key: "index" })).toBe(1)
  })

  test("deletePending without a task deletes the jobs of the scope of createTestJobs at the call, the unscoped jobs when it has none, and its own unscoped jobs with ALL_SCOPES", async () => {
    const unscoped = createTestJobs()
    await unscoped.enqueue(sendEmailTask, { to: "a" })
    await unscoped.enqueue(sendEmailTask, { to: "b" })
    expect(await unscoped.deletePending()).toBe(2)
    let scope = "a"
    const jobs = createTestJobs({ scope: () => scope })
    await jobs.enqueue(reportTask, { id: 1 })
    scope = "b"
    await jobs.enqueue(reportTask, { id: 2 })
    await jobs.enqueue(sendEmailTask, { to: "a" })
    expect(await jobs.deletePending()).toBe(1)
    const all = createTestJobs({ scope: ALL_SCOPES })
    await all.enqueue(sendEmailTask, { to: "a" })
    expect(await all.deletePending()).toBe(1)
  })

  test("retry resolves to null and deleteFailed to 0, since no job fails, after the checks of their arguments", async () => {
    const jobs = createTestJobs()
    await jobs.enqueue(sendEmailTask, { to: "a" })
    expect(await jobs.retry("1")).toBeNull()
    expect(await jobs.deleteFailed()).toBe(0)
    expect(await jobs.deleteFailed({ olderThan: 60 })).toBe(0)
    await expect(jobs.retry("0")).rejects.toThrow(RangeError)
    await expect(jobs.delete(1 as never)).rejects.toThrow(TypeError)
    await expect(jobs.deletePending(sendEmailTask as never)).rejects.toThrow(TypeError)
    await expect(jobs.deleteFailed({} as never)).rejects.toThrow(TypeError)
  })
})

describe("capture", () => {
  test("resolves to the enqueues that the action made and its result, without those of earlier steps", async () => {
    const jobs = createTestJobs()
    await jobs.enqueue(sendEmailTask, { to: "fixture" })
    const captured = await jobs.capture(async () => {
      await jobs.enqueue(sendEmailTask, { to: "a" })
      return 42
    })
    expect(captured.result).toBe(42)
    expect(captured.calls.map(call => call.payload)).toEqual([{ to: "a" }])
    expect(jobs.calls).toHaveLength(2)
  })

  test("nested and concurrent captures each record every enqueue made while they run", async () => {
    const jobs = createTestJobs()
    let release!: () => void
    const gate = new Promise<void>(resolve => (release = resolve))
    const outer = jobs.capture(async () => {
      await jobs.enqueue(sendEmailTask, { to: "1" })
      await gate
      const inner = await jobs.capture(() => jobs.enqueue(sendEmailTask, { to: "3" }))
      expect(inner.payloadsFor(sendEmailTask)).toEqual([{ to: "3" }])
    })
    const concurrent = await jobs.capture(() => jobs.enqueue(sendEmailTask, { to: "2" }))
    release()
    expect((await outer).payloadsFor(sendEmailTask)).toEqual([
      { to: "1" },
      { to: "2" },
      { to: "3" },
    ])
    expect(concurrent.payloadsFor(sendEmailTask)).toEqual([{ to: "2" }])
  })

  test("ends when an action that returns a value returns, so that an enqueue after capture stays out", async () => {
    const jobs = createTestJobs()
    const capturing = jobs.capture(() => 42)
    const enqueuing = jobs.enqueue(sendEmailTask, { to: "after" })
    const captured = await capturing
    await enqueuing
    expect(captured.result).toBe(42)
    expect(captured.calls).toEqual([])
  })

  test("awaits a thenable that the action returns", async () => {
    const jobs = createTestJobs()
    const captured = await jobs.capture(() => ({
      // oxlint-disable-next-line unicorn/no-thenable -- The test needs a thenable that is not a promise.
      then: (resolve: (value: number) => void) => {
        void jobs.enqueue(sendEmailTask, { to: "a" })
        resolve(7)
      },
    }))
    expect(captured.result).toBe(7)
    expect(captured.payloadsFor(sendEmailTask)).toEqual([{ to: "a" }])
  })

  test("rejects with the error of the action", async () => {
    const jobs = createTestJobs()
    const error = new Error("action")
    await expect(
      jobs.capture(async () => {
        await jobs.enqueue(sendEmailTask, { to: "a" })
        throw error
      }),
    ).rejects.toBe(error)
  })

  test("callsFor returns the enqueues of a scoped task in the scope of createTestJobs at the call", async () => {
    let scope = "a"
    const jobs = createTestJobs({ scope: () => scope })
    const captured = await jobs.capture(async () => {
      await jobs.enqueue(reportTask, { id: 1 })
      scope = "b"
      await jobs.enqueue(reportTask, { id: 2 })
    })
    expect(captured.payloadsFor(reportTask)).toEqual([{ id: 2 }])
    scope = "a"
    expect(captured.callsFor(reportTask)).toEqual([
      { task: "reports.build", scope: "a", payload: { id: 1 }, options: {} },
    ])
  })

  test("callsFor of a scoped task returns the enqueues of its name in every scope with ALL_SCOPES, such as those of an unscoped definition of the name, as deletePending does", async () => {
    const jobs = createTestJobs({ scope: ALL_SCOPES })
    const captured = await jobs.capture(() => jobs.enqueue(defineTask("shared")))
    expect(captured.callsFor(defineTask("shared", { scoped: true }))).toEqual([
      { task: "shared", scope: null, payload: undefined, options: {} },
    ])
  })

  test("callsFor throws TypeError for a scoped task when createTestJobs has no scope", async () => {
    const captured = await createTestJobs().capture(() => {})
    expect(() => captured.callsFor(reportTask)).toThrow(TypeError)
  })

  test("callsFor returns the enqueues of an unscoped task without a scope, and not those of a scoped definition of its name", async () => {
    const scopedTask = defineTask("shared", { scoped: true })
    const unscopedTask = defineTask("shared")
    const getScope = mock(() => "a")
    const jobs = createTestJobs({ scope: getScope })
    const captured = await jobs.capture(async () => {
      await jobs.enqueue(scopedTask)
      await jobs.enqueue(unscopedTask)
    })
    getScope.mockClear()
    expect(captured.callsFor(unscopedTask)).toEqual([
      { task: "shared", scope: null, payload: undefined, options: {} },
    ])
    expect(getScope).not.toHaveBeenCalled()
    expect(captured.callsFor(scopedTask).map(call => call.scope)).toEqual(["a"])
  })
})

describe("clear", () => {
  test("forgets the enqueues and the pending jobs, while a running capture keeps those it recorded", async () => {
    const jobs = createTestJobs()
    await jobs.enqueue(indexProductTask, { productId: "1" })
    const captured = await jobs.capture(async () => {
      await jobs.enqueue(sendEmailTask, { to: "a" })
      jobs.clear()
    })
    expect(jobs.calls).toEqual([])
    expect(captured.payloadsFor(sendEmailTask)).toEqual([{ to: "a" }])
    expect(await jobs.enqueue(indexProductTask, { productId: "1" })).toMatchObject({
      created: true,
    })
  })
})

describe("runJob", () => {
  test("runs the handler of a scoped task in the scope of createTestJobs at the call, as runJob does with the payload and the metadata", async () => {
    let scope = "a"
    const handler = mock((_payload: { id: number }, _context: HandlerContext<string>) => "done")
    const implementation = defineTask("t", { scoped: true, queue: "q" })
      .withPayload<{ id: number }>()
      .implement(handler)
    const jobs = createTestJobs({ scope: () => scope })
    expect(await jobs.runJob(implementation, { id: 1 })).toBe("done")
    scope = "b"
    await jobs.runJob(implementation, { id: 2 }, { job: { attempt: 3 } })
    expect(handler.mock.calls.map(([payload, context]) => [payload, context.job])).toEqual([
      [{ id: 1 }, { id: "1", scope: "a", attempt: 1, maxAttempts: 25, key: null, queue: "q" }],
      [{ id: 2 }, { id: "1", scope: "b", attempt: 3, maxAttempts: 25, key: null, queue: "q" }],
    ])
  })

  test("runs the handler of an unscoped task without a scope, without calling the function of the scope", async () => {
    const handler = mock((_payload: void, _context: HandlerContext) => {})
    const getScope = mock(() => "a")
    await createTestJobs({ scope: getScope }).runJob(defineTask("t").implement(handler))
    expect(handler.mock.calls[0]![1].job.scope).toBeNull()
    expect(getScope).not.toHaveBeenCalled()
  })

  test("takes the scope of options.job, without calling the function of the scope, and the signal of options", async () => {
    const handler = mock((_payload: void, _context: HandlerContext) => {})
    const getScope = mock(() => "a")
    const jobs = createTestJobs({ scope: getScope })
    const controller = new AbortController()
    await jobs.runJob(defineTask("t", { scoped: true }).implement(handler), undefined, {
      job: { scope: "b" },
      signal: controller.signal,
    })
    await jobs.runJob(defineTask("u").implement(handler), undefined, { job: { scope: "c" } })
    expect(handler.mock.calls.map(([, context]) => context.job.scope)).toEqual(["b", "c"])
    expect(handler.mock.calls[0]![1].signal).toBe(controller.signal)
    expect(getScope).not.toHaveBeenCalled()
  })

  test("runs the handler through runHandler of options in the scope of createTestJobs", async () => {
    const runHandler = mock<RunHandler>((_context, run) => run())
    const jobs = createTestJobs({ scope: "a" })
    expect(
      await jobs.runJob(
        reportTask.implement(() => "done"),
        { id: 1 },
        { runHandler },
      ),
    ).toBe("done")
    expect(runHandler.mock.calls[0]![0]).toMatchObject({
      task: "reports.build",
      job: { scope: "a" },
    })
  })

  test("reads the scope of options.job once, and passes the signal and the job of options from getters of a class, as runJob does", async () => {
    const handler = mock((_payload: void, _context: HandlerContext) => {})
    const controller = new AbortController()
    let scopeReads = 0
    class Job {
      get scope() {
        return `s${++scopeReads}`
      }
      get attempt() {
        return 2
      }
    }
    class Options {
      get job() {
        return new Job()
      }
      get signal() {
        return controller.signal
      }
    }
    await createTestJobs({ scope: "a" }).runJob(
      defineTask("t", { scoped: true }).implement(handler),
      undefined,
      new Options(),
    )
    expect(scopeReads).toBe(1)
    expect(handler.mock.calls[0]![1].job).toMatchObject({ scope: "s1", attempt: 2 })
    expect(handler.mock.calls[0]![1].signal).toBe(controller.signal)
  })

  test("rejects a scoped task without a scope or with ALL_SCOPES with TypeError, and an invalid scope of the function with its error, as enqueue does, before it runs the handler", async () => {
    const handler = mock(() => {})
    const implementation = reportTask.implement(handler)
    await expect(createTestJobs().runJob(implementation, { id: 1 })).rejects.toThrow(TypeError)
    await expect(
      createTestJobs({ scope: ALL_SCOPES }).runJob(implementation, { id: 1 }),
    ).rejects.toThrow(TypeError)
    await expect(
      createTestJobs({ scope: () => "" }).runJob(implementation, { id: 1 }),
    ).rejects.toThrow(RangeError)
    await expect(
      createTestJobs({ scope: "a" }).runJob(implementation, { id: 1 }, { job: { scope: null } }),
    ).rejects.toThrow(TypeError)
    expect(handler).not.toHaveBeenCalled()
  })
})

describe("runFailure", () => {
  test("runs the onFailure of a scoped task in the scope of createTestJobs at the call, as runFailure does", async () => {
    const onFailure = mock(
      (_failure: JobFailure<{ id: number }>, _context: HandlerContext<string>) => "done",
    )
    const implementation = defineTask("t", { scoped: true, queue: "q" })
      .withPayload<{ id: number }>()
      .implement(() => {}, { onFailure })
    let scope = "a"
    const jobs = createTestJobs({ scope: () => scope })
    expect(await jobs.runFailure(implementation, { id: 1 }, { reason: "lease-expired" })).toBe(
      "done",
    )
    scope = "b"
    await jobs.runFailure(implementation, { id: 2 })
    const [failure, context] = onFailure.mock.calls[0]!
    expect(failure).toMatchObject({
      reason: "lease-expired",
      job: { id: "1", task: "t", scope: "a", attempt: 25, maxAttempts: 25, key: null, queue: "q" },
      payload: { id: 1 },
    })
    expect(context.job).toMatchObject({ id: "2", scope: "a", attempt: 1, key: null, queue: null })
    expect(onFailure.mock.calls[1]![0].job.scope).toBe("b")
    expect(onFailure.mock.calls[1]![1].job.scope).toBe("b")
  })

  test("runs the onFailure of an unscoped task without a scope, and takes the scope of options.job, without calling the function of the scope", async () => {
    const onFailure = mock((_failure: JobFailure<void>, _context: HandlerContext) => {})
    const getScope = mock(() => "a")
    const jobs = createTestJobs({ scope: getScope })
    await jobs.runFailure(defineTask("t").implement(() => {}, { onFailure }))
    await jobs.runFailure(
      defineTask("u", { scoped: true }).implement(() => {}, { onFailure }),
      undefined,
      { job: { scope: "b" } },
    )
    expect(onFailure.mock.calls.map(([failure]) => failure.job.scope)).toEqual([null, "b"])
    expect(getScope).not.toHaveBeenCalled()
  })

  test("runs onFailure through runHandler of options in the scope of createTestJobs", async () => {
    const runHandler = mock<RunHandler>((_context, run) => run())
    const implementation = reportTask.implement(() => {}, { onFailure: () => "done" })
    const jobs = createTestJobs({ scope: "a" })
    expect(await jobs.runFailure(implementation, { id: 1 }, { runHandler })).toBe("done")
    expect(runHandler.mock.calls[0]![0]).toMatchObject({
      task: "reports.build:failure",
      job: { scope: "a" },
    })
  })

  test("rejects a scoped task without a scope or with ALL_SCOPES with TypeError, as enqueue does, and an implementation without onFailure with TypeError, before it runs onFailure", async () => {
    const onFailure = mock(() => {})
    const implementation = reportTask.implement(() => {}, { onFailure })
    await expect(createTestJobs().runFailure(implementation, { id: 1 })).rejects.toThrow(TypeError)
    await expect(
      createTestJobs({ scope: ALL_SCOPES }).runFailure(implementation, { id: 1 }),
    ).rejects.toThrow(TypeError)
    await expect(
      createTestJobs({ scope: "a" }).runFailure(reportTask.implement(() => {}) as never, { id: 1 }),
    ).rejects.toThrow(TypeError)
    expect(onFailure).not.toHaveBeenCalled()
  })
})
