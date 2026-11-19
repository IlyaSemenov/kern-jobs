// Runs of a handler in a test: the metadata of its job, its scope, its payload through JSON, its signal, and its outcome.
import { describe, expect, mock, test } from "bun:test"

import { defineTask, type HandlerContext } from "../task/task"

import { runJob } from "./run-job"

describe("job", () => {
  test("gives the key, the queue, and maxAttempts of the options of the task for the payload, the first attempt, and the id 1", async () => {
    const handler = mock((_payload: { id: number }, _context: HandlerContext) => {})
    const task = defineTask("t", { queue: "q", maxAttempts: 3 })
      .withPayload<{ id: number }>()
      .withOptions(payload => ({ key: `k:${payload.id}` }))
    await runJob(task.implement(handler), { id: 7 })
    expect(handler.mock.calls[0]![1].job).toEqual({
      id: "1",
      scope: null,
      attempt: 1,
      maxAttempts: 3,
      key: "k:7",
      queue: "q",
    })
  })

  test("gives 25 attempts, no key, and no queue for a task without options", async () => {
    const handler = mock((_payload: void, _context: HandlerContext) => {})
    await runJob(defineTask("t").implement(handler))
    expect(handler.mock.calls[0]![1].job).toMatchObject({ maxAttempts: 25, key: null, queue: null })
  })

  test("replaces the metadata by options.job, and keeps a default for a field given as undefined", async () => {
    const handler = mock((_payload: void, _context: HandlerContext) => {})
    await runJob(defineTask("t", { key: "k", queue: "q" }).implement(handler), undefined, {
      job: { id: "9", attempt: 3, maxAttempts: 3, key: null, queue: undefined },
    })
    expect(handler.mock.calls[0]![1].job).toEqual({
      id: "9",
      scope: null,
      attempt: 3,
      maxAttempts: 3,
      key: null,
      queue: "q",
    })
  })

  test("rejects with the errors of an enqueue for invalid options of the task, before it runs the handler", async () => {
    const handler = mock(() => {})
    const task = defineTask("t")
      .withPayload<number>()
      .withOptions(() => ({ delay: -1 }))
    await expect(runJob(task.implement(handler), 1)).rejects.toThrow(RangeError)
    expect(handler).not.toHaveBeenCalled()
  })
})

describe("scope", () => {
  const scopedTask = defineTask("t", { scoped: true })

  test("gives the scope of options.job to the job of a scoped task", async () => {
    const handler = mock((_payload: void, _context: HandlerContext<string>) => {})
    await runJob(scopedTask.implement(handler), undefined, { job: { scope: "a" } })
    expect(handler.mock.calls[0]![1].job.scope).toBe("a")
  })

  test("rejects a scoped task without a scope or with null with TypeError, and an empty scope with RangeError, before it runs the handler", async () => {
    const handler = mock(() => {})
    const implementation = scopedTask.implement(handler)
    await expect(runJob(implementation, undefined, {} as never)).rejects.toThrow(TypeError)
    await expect(
      runJob(implementation, undefined, { job: { scope: null } } as never),
    ).rejects.toThrow(TypeError)
    await expect(runJob(implementation, undefined, { job: { scope: "" } })).rejects.toThrow(
      RangeError,
    )
    expect(handler).not.toHaveBeenCalled()
  })

  test("gives a task that is not scoped null or the scope of options.job", async () => {
    const handler = mock((_payload: void, _context: HandlerContext) => {})
    const implementation = defineTask("t").implement(handler)
    await runJob(implementation, undefined, { job: { scope: null } })
    await runJob(implementation, undefined, { job: { scope: "a" } })
    expect(handler.mock.calls.map(([, context]) => context.job.scope)).toEqual([null, "a"])
    await expect(runJob(implementation, undefined, { job: { scope: "" } })).rejects.toThrow(
      RangeError,
    )
  })
})

describe("payload", () => {
  test("gives the handler the payload as a worker reads it from the table, through JSON", async () => {
    const handler = mock((_payload: unknown) => {})
    const implementation = defineTask("t").withPayload<unknown>().implement(handler)
    const date = new Date("2030-01-01T00:00:00Z")
    await runJob(implementation, { at: date, skipped: undefined })
    await runJob(implementation, undefined)
    await runJob(implementation, null)
    await runJob(implementation, { toJSON: () => undefined })
    expect(handler.mock.calls.map(([payload]) => payload)).toEqual([
      { at: "2030-01-01T00:00:00.000Z" },
      undefined,
      null,
      undefined,
    ])
  })

  test("rejects with the TypeError of JSON for a payload that an enqueue cannot serialize", async () => {
    const handler = mock(() => {})
    await expect(
      runJob(defineTask("t").withPayload<bigint>().implement(handler), 1n),
    ).rejects.toThrow(TypeError)
    expect(handler).not.toHaveBeenCalled()
  })
})

describe("outcome", () => {
  test("resolves to what the handler returns, and rejects with what it throws", async () => {
    const task = defineTask("t").withPayload<number>()
    expect(
      await runJob(
        task.implement(async payload => payload * 2),
        2,
      ),
    ).toBe(4)
    const error = new Error("handler")
    await expect(
      runJob(
        task.implement(() => {
          throw error
        }),
        1,
      ),
    ).rejects.toBe(error)
  })

  test("gives the handler the signal of options, or one that never aborts", async () => {
    const handler = mock((_payload: void, _context: HandlerContext) => {})
    const implementation = defineTask("t").implement(handler)
    const controller = new AbortController()
    await runJob(implementation, undefined, { signal: controller.signal })
    await runJob(implementation)
    expect(handler.mock.calls[0]![1].signal).toBe(controller.signal)
    expect(handler.mock.calls[1]![1].signal.aborted).toBe(false)
  })
})
