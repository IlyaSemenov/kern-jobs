// Runs of onFailure in a test: the JobFailure, its defaults and overrides, the context of the failure job, the scope, the payload through JSON, the outcome, and the run through runHandler.

import { describe, expect, mock, test } from "bun:test"

import type { JobFailure } from "../task/failure"
import type { RunHandlerContext } from "../task/run-handler"
import { defineTask, type HandlerContext } from "../task/task"

import { runFailure } from "./run-failure"

describe("failure", () => {
  test("gives the id 1, handler-error, a default error, the current time, the payload, and the metadata of a job of the payload on its last attempt", async () => {
    const onFailure = mock((_failure: JobFailure<{ id: number }>, _context: HandlerContext) => {})
    const task = defineTask("t", { queue: "q", maxAttempts: 3 })
      .withPayload<{ id: number }>()
      .withKey(payload => `k:${payload.id}`)
    const before = Date.now()
    await runFailure(
      task.implement(() => {}, { onFailure }),
      { id: 7 },
    )
    const failure = onFailure.mock.calls[0]![0]
    expect(failure).toEqual({
      id: "1",
      reason: "handler-error",
      error: { message: "job failed" },
      failedAt: expect.any(String),
      job: { id: "1", task: "t", scope: null, attempt: 3, maxAttempts: 3, key: "k:7", queue: "q" },
      payload: { id: 7 },
    })
    expect(failure.failedAt).toBe(new Date(failure.failedAt).toISOString())
    expect(Date.parse(failure.failedAt)).toBeGreaterThanOrEqual(before)
    expect(Date.parse(failure.failedAt)).toBeLessThanOrEqual(Date.now())
  })

  test("gives the context of the failure job: the id after that of the failed job, the first attempt, the attempts of onFailure, and no key or queue", async () => {
    const handler = mock((_failure: JobFailure, _context: HandlerContext) => {})
    const task = defineTask("t", { key: "k", queue: "q", maxAttempts: 3 })
    const implementation = task.implement(() => {}, { onFailure: { handler, maxAttempts: 7 } })
    await runFailure(implementation)
    // An id beyond the precision of a number.
    await runFailure(implementation, undefined, { job: { id: "9007199254740993" } })
    expect(handler.mock.calls[0]![1].job).toEqual({
      id: "2",
      scope: null,
      attempt: 1,
      maxAttempts: 7,
      key: null,
      queue: null,
    })
    expect(handler.mock.calls[1]![1].job.id).toBe("9007199254740994")
  })

  test("replaces the fields by options, and keeps a default for a field given as undefined", async () => {
    const onFailure = mock((_failure: JobFailure, _context: HandlerContext) => {})
    const implementation = defineTask("t", { key: "k", queue: "q" }).implement(() => {}, {
      onFailure,
    })
    await runFailure(implementation, undefined, {
      id: "5",
      reason: "lease-expired",
      error: { message: "lease expired" },
      failedAt: "2030-01-01T00:00:00.000Z",
      job: { id: "4", task: "u", attempt: 2, maxAttempts: 3, key: null, queue: undefined },
    })
    await runFailure(implementation, undefined, { reason: undefined, job: { maxAttempts: 4 } })
    expect(onFailure.mock.calls[0]![0]).toEqual({
      id: "5",
      reason: "lease-expired",
      error: { message: "lease expired" },
      failedAt: "2030-01-01T00:00:00.000Z",
      job: { id: "4", task: "u", scope: null, attempt: 2, maxAttempts: 3, key: null, queue: "q" },
      payload: undefined,
    })
    // The attempt follows maxAttempts of options.job.
    expect(onFailure.mock.calls[1]![0]).toMatchObject({
      reason: "handler-error",
      job: { attempt: 4, maxAttempts: 4 },
    })
  })

  test("gives the failed job of a batch task without a key of its own the key of options.job, as runJob does", async () => {
    const onFailure = mock((_failure: JobFailure<number[]>, _context: HandlerContext) => {})
    const implementation = defineTask("t")
      .withBatchPayload<number>({ maxItems: 3 })
      .implement(() => {}, { onFailure })
    await runFailure(implementation, [1], { job: { key: "" } })
    expect(onFailure.mock.calls[0]![0]).toMatchObject({ job: { key: "" }, payload: [1] })
  })

  test("gives the error message that the worker records for a reason, or job failed for an error of the handler, and the message of options.error, also of an Error", async () => {
    const onFailure = mock((_failure: JobFailure, _context: HandlerContext) => {})
    const implementation = defineTask("t").implement(() => {}, { onFailure })
    for (const reason of [
      "handler-error",
      "terminal-error",
      "lease-expired",
      "worker-restarted",
      "late-claim",
    ] as const) {
      await runFailure(implementation, undefined, { reason })
    }
    await runFailure(implementation, undefined, {
      reason: "lease-expired",
      error: new Error("declined"),
    })
    expect(onFailure.mock.calls.map(([failure]) => failure.error)).toEqual([
      { message: "job failed" },
      { message: "job failed" },
      { message: "lease expired" },
      { message: "worker restarted" },
      { message: "lease confirmation deadline passed before the handler started" },
      { message: "declined" },
    ])
  })

  test("reads each field of options and options.job once, also of a getter of a class", async () => {
    const onFailure = mock((_failure: JobFailure, _context: HandlerContext) => {})
    const reads: string[] = []
    const controller = new AbortController()
    const read = <T>(field: string, value: T) => {
      reads.push(field)
      return value
    }
    class Job {
      get scope() {
        return read("job.scope", `s${reads.filter(field => field === "job.scope").length + 1}`)
      }
      get task() {
        return read("job.task", "u")
      }
      get attempt() {
        return read("job.attempt", 2)
      }
    }
    class Options {
      job = new Job()
      get signal() {
        return read("signal", controller.signal)
      }
      get reason() {
        return read("reason", "late-claim" as const)
      }
      get failedAt() {
        return read("failedAt", "2030-01-01T00:00:00.000Z")
      }
    }
    await runFailure(
      defineTask("t").implement(() => {}, { onFailure }),
      undefined,
      new Options(),
    )
    expect(reads.toSorted()).toEqual([
      "failedAt",
      "job.attempt",
      "job.scope",
      "job.task",
      "reason",
      "signal",
    ])
    expect(onFailure.mock.calls[0]![0]).toMatchObject({
      reason: "late-claim",
      failedAt: "2030-01-01T00:00:00.000Z",
      job: { task: "u", scope: "s1", attempt: 2 },
    })
    expect(onFailure.mock.calls[0]![1].job.scope).toBe("s1")
    expect(onFailure.mock.calls[0]![1].signal).toBe(controller.signal)
  })

  test("rejects with TypeError for an implementation without onFailure, before it reads the options", async () => {
    const options = {
      get job(): never {
        throw new Error("read")
      },
    }
    await expect(
      runFailure(defineTask("t").implement(() => {}) as never, undefined, options),
    ).rejects.toThrow(new TypeError("Task t has no onFailure"))
  })

  test("rejects a job.id that is not the id of a job with RangeError, as runJob does, before it runs onFailure", async () => {
    const onFailure = mock(() => {})
    await expect(
      runFailure(
        defineTask("t").implement(() => {}, { onFailure }),
        undefined,
        {
          job: { id: "job-1" },
        },
      ),
    ).rejects.toThrow(RangeError)
    expect(onFailure).not.toHaveBeenCalled()
  })

  test("rejects with the errors of an enqueue for invalid options of the task, before it reads the other fields of options.job and runs onFailure", async () => {
    const onFailure = mock(() => {})
    const getTask = mock(() => "u")
    const task = defineTask("t")
      .withPayload<number>()
      .withOptions(() => ({ delay: -1 }))
    const job = {
      get task() {
        return getTask()
      },
    }
    await expect(
      runFailure(
        task.implement(() => {}, { onFailure }),
        1,
        { job },
      ),
    ).rejects.toThrow(RangeError)
    expect(getTask).not.toHaveBeenCalled()
    expect(onFailure).not.toHaveBeenCalled()
  })
})

describe("scope", () => {
  const scopedTask = defineTask("t", { scoped: true })

  test("gives the scope of options.job to the failed job and the failure job of a scoped task", async () => {
    const onFailure = mock((_failure: JobFailure<void>, _context: HandlerContext<string>) => {})
    await runFailure(
      scopedTask.implement(() => {}, { onFailure }),
      undefined,
      {
        job: { scope: "a" },
      },
    )
    expect(onFailure.mock.calls[0]![0].job.scope).toBe("a")
    expect(onFailure.mock.calls[0]![1].job.scope).toBe("a")
  })

  test("rejects a scoped task without a scope with TypeError, and an empty scope with RangeError, before it runs onFailure", async () => {
    const onFailure = mock(() => {})
    const implementation = scopedTask.implement(() => {}, { onFailure })
    await expect(runFailure(implementation, undefined, {} as never)).rejects.toThrow(
      new TypeError("Task t is scoped: pass the scope of its job as job.scope"),
    )
    await expect(runFailure(implementation, undefined, { job: { scope: "" } })).rejects.toThrow(
      RangeError,
    )
    expect(onFailure).not.toHaveBeenCalled()
  })

  test("gives a task that is not scoped null or the scope of options.job", async () => {
    const onFailure = mock((_failure: JobFailure<void>, _context: HandlerContext) => {})
    const implementation = defineTask("t").implement(() => {}, { onFailure })
    await runFailure(implementation)
    await runFailure(implementation, undefined, { job: { scope: "a" } })
    expect(
      onFailure.mock.calls.map(([failure, context]) => [failure.job.scope, context.job.scope]),
    ).toEqual([
      [null, null],
      ["a", "a"],
    ])
  })
})

describe("payload", () => {
  test("gives onFailure the payload as a worker reads the failure from the table, through JSON", async () => {
    const onFailure = mock((_failure: JobFailure<unknown>) => {})
    const implementation = defineTask("t")
      .withPayload<unknown>()
      .implement(() => {}, { onFailure })
    await runFailure(implementation, { at: new Date("2030-01-01T00:00:00Z"), skipped: undefined })
    await runFailure(implementation, undefined)
    await runFailure(implementation, null)
    expect(onFailure.mock.calls.map(([failure]) => failure.payload)).toEqual([
      { at: "2030-01-01T00:00:00.000Z" },
      undefined,
      null,
    ])
  })

  test("serializes the payload as the root of its JSON, as an enqueue stores it", async () => {
    const onFailure = mock((_failure: JobFailure<unknown>) => {})
    const implementation = defineTask("t")
      .withPayload<unknown>()
      .implement(() => {}, { onFailure })
    await runFailure(implementation, { toJSON: (key: string) => `key:${key}` })
    await runFailure(implementation, { toJSON: () => undefined })
    expect(onFailure.mock.calls.map(([failure]) => failure.payload)).toEqual(["key:", undefined])
    expect("payload" in onFailure.mock.calls[1]![0]).toBe(false)
  })

  test("rejects with the TypeError of JSON for a payload that an enqueue cannot serialize", async () => {
    const onFailure = mock(() => {})
    await expect(
      runFailure(
        defineTask("t")
          .withPayload<bigint>()
          .implement(() => {}, { onFailure }),
        1n,
      ),
    ).rejects.toThrow(TypeError)
    expect(onFailure).not.toHaveBeenCalled()
  })
})

describe("outcome", () => {
  test("resolves to what onFailure returns, and rejects with what it throws, without running the handler", async () => {
    const handler = mock(() => {})
    const task = defineTask("t").withPayload<number>()
    expect(
      await runFailure(
        task.implement(handler, { onFailure: async failure => failure.payload * 2 }),
        2,
      ),
    ).toBe(4)
    const error = new Error("onFailure")
    await expect(
      runFailure(
        task.implement(handler, {
          onFailure: () => {
            throw error
          },
        }),
        1,
      ),
    ).rejects.toBe(error)
    expect(handler).not.toHaveBeenCalled()
  })

  test("gives onFailure the signal of options, or one that never aborts", async () => {
    const onFailure = mock((_failure: JobFailure<void>, _context: HandlerContext) => {})
    const implementation = defineTask("t").implement(() => {}, { onFailure })
    const controller = new AbortController()
    await runFailure(implementation, undefined, { signal: controller.signal })
    await runFailure(implementation)
    expect(onFailure.mock.calls[0]![1].signal).toBe(controller.signal)
    expect(onFailure.mock.calls[1]![1].signal.aborted).toBe(false)
  })
})

describe("runHandler", () => {
  test("runs onFailure through runHandler with the context of the failure job, the name of the failure task, and the failure as the payload", async () => {
    const onFailure = mock((_failure: JobFailure<void>, _context: HandlerContext<string>) => "done")
    const contexts: RunHandlerContext[] = []
    const result = await runFailure(
      defineTask("t", { scoped: true }).implement(() => {}, { onFailure }),
      undefined,
      {
        job: { scope: "a" },
        runHandler: async (context, run) => {
          contexts.push(context)
          return [await run()]
        },
      },
    )
    expect(result).toEqual(["done"])
    const [failure, context] = onFailure.mock.calls[0]!
    expect(contexts).toEqual([
      { task: "t:failure", payload: failure, job: context.job, signal: context.signal },
    ])
    expect(context.job).toMatchObject({ id: "2", scope: "a" })
  })

  test("resolves to what runHandler resolves to without running onFailure when it does not call run", async () => {
    const onFailure = mock(() => {})
    const implementation = defineTask("t").implement(() => {}, { onFailure })
    expect(await runFailure(implementation, undefined, { runHandler: () => "skipped" })).toBe(
      "skipped",
    )
    expect(onFailure).not.toHaveBeenCalled()
  })
})
