// Tasks of a worker from a manifest: the handler of an implementation with the payload and metadata of its job, the lease and backoff of its task, and the check of the manifest; and a worker that runs them.
import { afterEach, beforeEach, describe, expect, test } from "bun:test"

import { enqueue } from "../core/enqueue"
import type { JobRow } from "../core/job"
import { createTestSchema, jobs, only, type TestSchema } from "../core/testing"
import { fixedBackoff } from "../task/backoff"
import { TerminalError } from "../task/errors"
import { defineManifest } from "../task/manifest"
import { defineTask, type HandlerContext } from "../task/task"

import { toWorkerTasks } from "./tasks"
import { recordEvents, startTestWorker, stopTestWorkers } from "./testing"

const job: JobRow = {
  id: "1",
  task: "search.indexProduct",
  scope: "",
  queue: "search",
  key: "product:42",
  key_mode: "replace",
  payload: { productId: "42" },
  run_at: new Date(0),
  not_before: null,
  window_started_at: new Date(0),
  created_at: new Date(0),
  attempts: 2,
  max_attempts: 5,
  state: "active",
  claim_id: "7",
  locked_by: "w",
  locked_at: new Date(0),
  lease_until: new Date(60_000),
  last_error: null,
  failed_at: null,
}

describe("toWorkerTasks", () => {
  test("runs the handler of an implementation with the payload and metadata of its job, under the lease and backoff of its task", async () => {
    const backoff = fixedBackoff(7)
    const calls: [unknown, HandlerContext][] = []
    const implementation = defineTask("search.indexProduct", { lease: 30, backoff })
      .withPayload<{ productId: string }>()
      .implement((payload, context) => {
        calls.push([payload, context])
        return "done"
      })
    const task = toWorkerTasks(defineManifest({ tasks: [implementation] }))["search.indexProduct"]!
    const signal = new AbortController().signal
    expect(await task.handler(job, { signal })).toBe("done")
    expect(calls).toEqual([
      [
        { productId: "42" },
        {
          signal,
          job: {
            id: "1",
            scope: null,
            attempt: 2,
            maxAttempts: 5,
            key: "product:42",
            queue: "search",
          },
        },
      ],
    ])
    expect([task.lease, task.backoff]).toEqual([30, backoff])
  })

  test("gives the handler the scope of its job, and null for the empty scope of a job without one", async () => {
    const scopes: (string | null)[] = []
    const implementation = defineTask("search.indexProduct").implement((_, { job }) => {
      scopes.push(job.scope)
    })
    const task = toWorkerTasks(defineManifest({ tasks: [implementation] }))["search.indexProduct"]!
    const signal = new AbortController().signal
    await task.handler({ ...job, scope: "tenant" }, { signal })
    await task.handler(job, { signal })
    expect(scopes).toEqual(["tenant", null])
  })

  test("throws a TerminalError for a job of a scoped task without a scope, without running its handler", () => {
    let calls = 0
    const implementation = defineTask("search.indexProduct", { scoped: true }).implement(() => {
      calls++
    })
    const task = toWorkerTasks(defineManifest({ tasks: [implementation] }))["search.indexProduct"]!
    let error: unknown
    try {
      task.handler(job, { signal: new AbortController().signal })
    } catch (e) {
      error = e
    }
    // The class, which `toThrow` of an error does not compare, decides that the job is not retried.
    expect(error).toBeInstanceOf(TerminalError)
    expect((error as Error).message).toBe(
      "task search.indexProduct is scoped, but its job has no scope",
    )
    expect(calls).toBe(0)
  })

  test("checks a manifest that defineManifest did not check", () => {
    const implementation = defineTask("t").implement(() => {})
    expect(() => toWorkerTasks({ tasks: [implementation, implementation] })).toThrow(
      "duplicate tasks: t",
    )
  })

  test("checks the settings of a task that defineTask did not check", () => {
    // Objects of the shape of a task pass the types without defineTask.
    const withSettings = (settings: object) => ({
      tasks: [{ task: { ...defineTask("accounts.sync"), ...settings }, handler() {} }],
    })
    expect(() => toWorkerTasks(withSettings({ lease: 1e13 }))).toThrow(
      new RangeError(
        "lease of task accounts.sync must be a number of seconds above 0 up to 1e12, got 10000000000000",
      ),
    )
    expect(() => toWorkerTasks(withSettings({ backoff: 5 }) as never)).toThrow(
      new TypeError("backoff of task accounts.sync must be a function of the attempt"),
    )
    expect(() => toWorkerTasks(withSettings({ scoped: "yes" }) as never)).toThrow(
      new TypeError("scoped of task accounts.sync must be a boolean"),
    )
  })

  test("keeps a task named __proto__ as a task", () => {
    const tasks = toWorkerTasks({ tasks: [defineTask("__proto__").implement(() => {})] })
    expect(Object.keys(tasks)).toEqual(["__proto__"])
    expect(Object.getPrototypeOf(tasks)).toBe(Object.prototype)
  })
})

describe("a worker of a manifest", () => {
  let schema: TestSchema
  beforeEach(async () => {
    schema = await createTestSchema()
  })
  afterEach(async () => {
    await stopTestWorkers()
    await schema.dispose()
  })

  test("claims under the lease of the task and gives handlers their payloads, undefined for a job without one", async () => {
    const gate = Promise.withResolvers<void>()
    const payloads = new Map<string, unknown>()
    const index = defineTask("search.indexProduct", { lease: 5 })
      .withPayload<{ productId: string }>()
      .implement(async (payload, { job }) => {
        payloads.set(job.id, payload)
        await gate.promise
      })
    const purge = defineTask("sessions.purgeExpired").implement((payload, { job }) => {
      payloads.set(job.id, payload)
    })
    const indexed = await enqueue(schema.admin, schema.table, {
      task: "search.indexProduct",
      payload: { productId: "42" },
    })
    const purged = await enqueue(schema.admin, schema.table, { task: "sessions.purgeExpired" })
    const log = recordEvents()
    try {
      startTestWorker(schema, {
        concurrency: 2,
        tasks: toWorkerTasks(defineManifest({ tasks: [index, purge] })),
        onEvent: log.onEvent,
      })
      expect((await log.waitFor("settled", e => e.claim.id === purged.id)).outcome).toBe(
        "completed",
      )
      await log.waitFor("start", e => e.claim.id === indexed.id)
      const row = only(await jobs(schema, `id = ${indexed.id}`))
      // The task lease of 5 seconds, not the test lease of the worker, of 1 second.
      expect(row.lease_until!.getTime() - row.locked_at!.getTime()).toBeGreaterThanOrEqual(4900)
    } finally {
      gate.resolve()
    }
    expect((await log.waitFor("settled", e => e.claim.id === indexed.id)).outcome).toBe("completed")
    expect(payloads).toEqual(
      new Map<string, unknown>([
        [indexed.id, { productId: "42" }],
        [purged.id, undefined],
      ]),
    )
  })
})
