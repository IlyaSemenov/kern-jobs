// The test double of the jobs API: recorded enqueues and their options, its pending jobs, captures, and the queries of a capture by task and scope.
import { describe, expect, mock, test } from "bun:test"

import { ALL_SCOPES } from "../jobs/jobs"
import { defineTask } from "../task/task"

import { createTestJobs } from "./test-jobs"

const sendEmailTask = defineTask("email.send").withPayload<{ to: string }>()
const indexProductTask = defineTask("search.indexProduct", {
  key: "index",
  queue: "search",
}).withPayload<{ productId: string }>()
const reportTask = defineTask("reports.build", { scoped: true }).withPayload<{ id: number }>()

describe("enqueue", () => {
  test("records the task, the scope, the payload as passed, and the options of defineTask, withOptions, and the overrides as an enqueue merges them", async () => {
    const date = new Date("2030-01-01T00:00:00Z")
    const optionsFor = mock((payload: { at: Date }) => ({
      delay: 5,
      key: `k:${payload.at.getTime()}`,
    }))
    const task = defineTask("t", { maxAttempts: 3, queue: "q" })
      .withPayload<{ at: Date }>()
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
