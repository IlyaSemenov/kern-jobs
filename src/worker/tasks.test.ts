// Tasks of a worker from the task implementations of a manifest: the handler of an implementation with the payload and metadata of its job, through `runHandler`, the lease and backoff of its task, and the check of the list; and a worker that runs them.

import { afterEach, beforeEach, describe, expect, test } from "bun:test"

import { sql } from "kern-orm/db"

import { enqueue } from "../sql/enqueue"
import { fixedBackoff } from "../task/backoff"
import { RetryError, TerminalError } from "../task/errors"
import type { JobFailure } from "../task/failure"
import type { RunHandlerContext } from "../task/run-handler"
import { defineTask, type HandlerContext, type JobInfo } from "../task/task"
import { createTestSchema, jobs, only, sleep, type TestSchema } from "../test-harness/database"

import { type Claim, type ClaimedJob, claimPlain } from "./db/claim"
import { toWorkerTasks } from "./tasks"
import { recordEvents, startTestWorker, stopTestWorkers } from "./test-harness"

const job: ClaimedJob = {
  id: "1",
  claimId: "7",
  task: "search.indexProduct",
  scope: "",
  key: "product:42",
  queue: "search",
  attempts: 2,
  maxAttempts: 5,
  payload: { productId: "42" },
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
    const task = toWorkerTasks([implementation])["search.indexProduct"]!
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
    const task = toWorkerTasks([implementation])["search.indexProduct"]!
    const signal = new AbortController().signal
    await task.handler({ ...job, scope: "tenant" }, { signal })
    await task.handler(job, { signal })
    expect(scopes).toEqual(["tenant", null])
  })

  test("throws a TerminalError for a job of a scoped task without a scope, without running its handler or runHandler", () => {
    let calls = 0
    const implementation = defineTask("search.indexProduct", { scoped: true }).implement(() => {
      calls++
    })
    const task = toWorkerTasks([implementation], (_, run) => {
      calls++
      return run()
    })["search.indexProduct"]!
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

  test("runs the handler through runHandler, which gets the context of the handler, the task, and the payload, and decides the outcome", async () => {
    const contexts: RunHandlerContext[] = []
    const failure = new Error("the index is down")
    const implementation = defineTask("search.indexProduct")
      .withPayload<{ productId: string }>()
      .implement((_, { job }) => {
        if (job.attempt > 2) throw failure
        return `indexed in ${job.scope}`
      })
    const task = toWorkerTasks([implementation], async (context, run) => {
      contexts.push(context)
      return `${await run()}, wrapped`
    })["search.indexProduct"]!
    const signal = new AbortController().signal
    expect(await task.handler({ ...job, scope: "tenant" }, { signal })).toBe(
      "indexed in tenant, wrapped",
    )
    expect(contexts).toEqual([
      {
        task: "search.indexProduct",
        payload: { productId: "42" },
        signal,
        job: {
          id: "1",
          scope: "tenant",
          attempt: 2,
          maxAttempts: 5,
          key: "product:42",
          queue: "search",
        },
      },
    ])
    await expect(task.handler({ ...job, attempts: 3 }, { signal })).rejects.toBe(failure)
  })

  test("runs no handler when runHandler does not call run", async () => {
    let calls = 0
    const implementation = defineTask("search.indexProduct").implement(() => {
      calls++
    })
    const task = toWorkerTasks([implementation], () => {})["search.indexProduct"]!
    expect(await task.handler(job, { signal: new AbortController().signal })).toBeUndefined()
    expect(calls).toBe(0)
  })

  test("checks a list of implementations that defineManifest did not check", () => {
    const implementation = defineTask("t").implement(() => {})
    expect(() => toWorkerTasks([implementation, implementation])).toThrow("duplicate tasks: t")
  })

  test("checks the name and the settings of a task that defineTask did not check", () => {
    // Objects of the shape of a task pass the types without defineTask.
    const withSettings = (settings: object) => [
      { task: { ...defineTask("accounts.sync"), ...settings }, handler() {} },
    ]
    expect(() => toWorkerTasks(withSettings({ name: "accounts.sync\ud800" }))).toThrow(
      "The name of a task must be",
    )
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

  test("gives a task with onFailure the attempt budget of its failure jobs, and adds its failure task, which runs onFailure and has no failure jobs of its own", async () => {
    const seen: unknown[] = []
    const backoff = fixedBackoff(5)
    const tasks = toWorkerTasks([
      defineTask("t", { lease: 60, backoff: fixedBackoff(1) }).implement(() => {}, {
        onFailure: (failure, context) => void seen.push([failure, context.job.id]),
      }),
      defineTask("u").implement(() => {}, {
        onFailure: { handler: () => {}, maxAttempts: 100, backoff },
      }),
      defineTask("v").implement(() => {}),
    ])
    expect(Object.keys(tasks)).toEqual(["t", "t:failure", "u", "u:failure", "v"])
    expect(
      [tasks.t, tasks.u, tasks.v, tasks["t:failure"], tasks["u:failure"]].map(
        task => task!.failureMaxAttempts,
      ),
    ).toEqual([25, 100, undefined, undefined, undefined])
    // The failure task takes neither the lease nor the backoff of the task, and has the backoff of onFailure.
    const { handler, ...settings } = tasks["t:failure"]!
    expect(settings).toEqual({ lease: undefined, backoff: undefined })
    expect(tasks["u:failure"]!.backoff).toBe(backoff)
    await handler(
      { ...job, task: "t:failure", payload: { id: "5" } },
      { signal: new AbortController().signal },
    )
    expect(seen).toEqual([[{ id: "5" }, job.id]])
  })

  test("registers a failure implementation listed on its own, without the task and without failure jobs of its own", () => {
    const { failure } = defineTask("t").implement(() => {}, { onFailure: () => "handled" })
    const tasks = toWorkerTasks([failure])
    expect(Object.keys(tasks)).toEqual(["t:failure"])
    expect(tasks["t:failure"]!.failureMaxAttempts).toBeUndefined()
  })

  test("runs the failure task of a scoped task as a scoped task, through runHandler", async () => {
    const wrapped: string[] = []
    const tasks = toWorkerTasks(
      [defineTask("t", { scoped: true }).implement(() => {}, { onFailure: () => "handled" })],
      (context, run) => {
        wrapped.push(`${context.task} of ${context.job.scope}`)
        return run()
      },
    )
    const signal = new AbortController().signal
    const failureJob = { ...job, task: "t:failure" }
    expect(await tasks["t:failure"]!.handler({ ...failureJob, scope: "acme" }, { signal })).toBe(
      "handled",
    )
    expect(wrapped).toEqual(["t:failure of acme"])
    expect(() => tasks["t:failure"]!.handler(failureJob, { signal })).toThrow(
      new TerminalError("task t:failure is scoped, but its job has no scope"),
    )
  })

  test("checks a failure task that implement did not make", () => {
    const task = defineTask("t")
    const { failure } = defineTask("u").implement(() => {}, { onFailure: () => {} })
    for (const invalid of [{}, failure, task]) {
      expect(() => toWorkerTasks([{ task, handler() {}, failure: invalid as never }])).toThrow(
        new TypeError(
          "failure of the implementation of task t must be the implementation of task t:failure that implement makes of onFailure",
        ),
      )
    }
    const own = task.implement(() => {}, { onFailure: () => {} })
    const settings = { ...own.failure.task, lease: 0 }
    expect(() => toWorkerTasks([{ ...own, failure: { ...own.failure, task: settings } }])).toThrow(
      "lease of task t:failure must be",
    )
    expect(() => toWorkerTasks([own, own.failure])).toThrow("duplicate tasks: t:failure")
  })

  test("reads the task and the failure implementation of each implementation, and the task of the failure implementation, once, so that it checks the values that it runs", () => {
    const own = defineTask("t").implement(() => {}, {
      onFailure: { handler: () => {}, maxAttempts: 3 },
    })
    const reads = { task: 0, failure: 0, failureTask: 0, scoped: 0 }
    const implementation = {
      get task() {
        reads.task++
        return {
          ...own.task,
          get scoped() {
            reads.scoped++
            return false
          },
        }
      },
      handler() {},
      get failure() {
        reads.failure++
        return {
          handler: own.failure.handler,
          get task() {
            reads.failureTask++
            return own.failure.task
          },
        }
      },
    }
    const tasks = toWorkerTasks([implementation])
    expect(tasks.t!.failureMaxAttempts).toBe(3)
    expect(reads).toEqual({ task: 1, failure: 1, failureTask: 1, scoped: 1 })
    // A failure task whose name changes after its check runs under the checked name, under which the failure jobs are inserted.
    const names: `${string}:failure`[] = ["t:failure", "other:failure"]
    const renamed = {
      ...own,
      failure: {
        handler: own.failure.handler,
        task: {
          ...own.failure.task,
          get name() {
            return names.shift()!
          },
        },
      },
    }
    expect(Object.keys(toWorkerTasks([renamed]))).toEqual(["t", "t:failure"])
    // A failure implementation that is absent at its one read gives no failure task, so another implementation may take the name.
    const values = [undefined, own.failure]
    const flaky = {
      task: own.task,
      handler() {},
      get failure() {
        return values.shift()
      },
    }
    expect(toWorkerTasks([flaky, own.failure]).t!.failureMaxAttempts).toBeUndefined()
  })

  test("rejects a failure task of a failure task", () => {
    const { failure } = defineTask("t").implement(() => {}, { onFailure: () => {} })
    const chained = { ...failure, failure: { task: defineTask("u"), handler() {} } }
    expect(() => toWorkerTasks([chained as never])).toThrow(
      new TypeError("Failure task t:failure takes no onFailure of its own"),
    )
  })

  test("keeps a task named __proto__ as a task", () => {
    const tasks = toWorkerTasks([defineTask("__proto__").implement(() => {})])
    expect(Object.keys(tasks)).toEqual(["__proto__"])
    expect(Object.getPrototypeOf(tasks)).toBe(Object.prototype)
  })
})

describe("a worker of a manifest", () => {
  let testSchema: TestSchema
  beforeEach(async () => {
    testSchema = await createTestSchema()
  })
  afterEach(async () => {
    await stopTestWorkers()
    await testSchema.dispose()
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
    const indexed = await enqueue(testSchema.conn, testSchema.table, {
      task: "search.indexProduct",
      payload: { productId: "42" },
    })
    const purged = await enqueue(testSchema.conn, testSchema.table, {
      task: "sessions.purgeExpired",
    })
    const log = recordEvents()
    try {
      startTestWorker(testSchema, {
        concurrency: 2,
        tasks: toWorkerTasks([index, purge]),
        onEvent: log.onEvent,
      })
      expect((await log.waitFor("settled", e => e.claim.id === purged.id)).outcome).toBe(
        "completed",
      )
      await log.waitFor("start", e => e.claim.id === indexed.id)
      const row = only(await jobs(testSchema.conn.query, sql`id = ${indexed.id}`))
      // The task lease of 5 seconds, not the test lease of the worker, of 1 second.
      expect(row.leaseUntil!.getTime() - row.lockedAt!.getTime()).toBeGreaterThanOrEqual(4900)
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

  describe("failure tasks", () => {
    const sourceTask = defineTask("orders.charge").withPayload<{ orderId: string }>()

    /** Starts a worker of an implementation of the task whose `onFailure` records each failure and the metadata of its failure job, after `onFailure` of the options, and whose `runHandler` records the task of each job. */
    function startFailureWorker(
      handler: Parameters<typeof sourceTask.implement>[0],
      o: { onFailure?: () => void } = {},
    ) {
      const seen: { failure: JobFailure<{ orderId: string }>; job: JobInfo }[] = []
      const wrapped: string[] = []
      const log = recordEvents()
      startTestWorker(testSchema, {
        concurrency: 2,
        tasks: toWorkerTasks(
          [
            sourceTask.implement(handler, {
              onFailure: (failure, { job }) => {
                o.onFailure?.()
                seen.push({ failure, job })
              },
            }),
          ],
          (context, run) => {
            wrapped.push(context.task)
            return run()
          },
        ),
        onEvent: log.onEvent,
      })
      return { seen, wrapped, log }
    }

    test("a job that fails for good adds a failure job, which runs onFailure with the JobFailure of the stored row and the context of its own job", async () => {
      const source = await enqueue(testSchema.conn, testSchema.table, {
        task: "orders.charge",
        payload: { orderId: "42" },
        maxAttempts: 1,
      })
      const { seen, wrapped, log } = startFailureWorker(payload => {
        // The JobFailure takes the payload as stored, not as the handler changed it.
        payload.orderId = "changed"
        throw new Error("card declined")
      })
      const start = await log.waitFor("start", e => e.claim.id === source.id)
      await log.waitFor("settled", e => e.claim.job.task === "orders.charge:failure")
      const { failure, job } = only(seen)
      expect(wrapped).toEqual(["orders.charge", "orders.charge:failure"])
      expect(failure).toEqual({
        id: start.claim.claimId,
        reason: "handler-error",
        error: { message: "card declined" },
        failedAt: expect.stringMatching(/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z$/),
        job: {
          id: source.id,
          task: "orders.charge",
          scope: null,
          attempt: 1,
          maxAttempts: 1,
          key: null,
          queue: null,
        },
        payload: { orderId: "42" },
      })
      expect(job).toMatchObject({
        scope: null,
        attempt: 1,
        maxAttempts: 25,
        key: null,
        queue: null,
      })
      expect(job.id).not.toBe(source.id)
      expect((await jobs(testSchema.conn.query)).map(r => [r.task, r.state])).toEqual([
        ["orders.charge", "failed"],
      ])
    })

    test("records the reason of each failure for good of a handler", async () => {
      const cases = [
        { maxAttempts: 5, thrown: new TerminalError("bad order"), reason: "terminal-error" },
        {
          maxAttempts: 1,
          thrown: new RetryError("rate limited", { delay: 1 }),
          reason: "handler-error",
        },
      ] as const
      const ids = await Promise.all(
        cases.map(({ maxAttempts }, i) =>
          enqueue(testSchema.conn, testSchema.table, {
            task: "orders.charge",
            payload: { orderId: String(i) },
            maxAttempts,
          }),
        ),
      )
      const { seen, log } = startFailureWorker(payload => {
        throw cases[Number(payload.orderId)]!.thrown
      })
      for (const { id } of ids) {
        await log.waitFor("settled", e => (e.claim.job.payload as JobFailure).job?.id === id)
      }
      const reasons = new Map(seen.map(({ failure }) => [failure.job.id, failure.reason]))
      expect(ids.map(({ id }) => reasons.get(id))).toEqual(cases.map(c => c.reason))
    })

    test("a failure job of a scoped task has the scope of the failed job, and that of a job without a scope fails for good without running onFailure", async () => {
      const scopedSource = defineTask("orders.charge", { scoped: true }).withPayload<{
        orderId: string
      }>()
      const seen: [JobFailure, string][] = []
      for (const scope of ["acme", undefined]) {
        await enqueue(testSchema.conn, testSchema.table, {
          task: "orders.charge",
          scope,
          maxAttempts: 1,
        })
      }
      const log = recordEvents()
      startTestWorker(testSchema, {
        concurrency: 2,
        tasks: toWorkerTasks([
          scopedSource.implement(
            () => {
              throw new Error("card declined")
            },
            { onFailure: (failure, { job }) => void seen.push([failure, job.scope]) },
          ),
        ]),
        onEvent: log.onEvent,
      })
      const isFailureJob = (e: { claim: Claim }) => e.claim.job.task === "orders.charge:failure"
      await log.waitFor("settled", e => isFailureJob(e) && e.outcome === "completed")
      await log.waitFor("settled", e => isFailureJob(e) && e.outcome === "failed")
      const [failure, scope] = only(seen)
      expect([failure.job.scope, failure.reason, scope]).toEqual(["acme", "handler-error", "acme"])
      const failed = only(await jobs(testSchema.conn.query, sql`task = 'orders.charge:failure'`))
      expect([failed.state, failed.scope, failed.lastError]).toEqual([
        "failed",
        "",
        "task orders.charge:failure is scoped, but its job has no scope",
      ])
      expect(failed.payload).toMatchObject({ reason: "terminal-error", job: { scope: null } })
    })

    test("a job interrupted by stop on its last attempt returns to pending and adds no failure job", async () => {
      const source = await enqueue(testSchema.conn, testSchema.table, {
        task: "orders.charge",
        payload: { orderId: "1" },
        maxAttempts: 1,
      })
      const log = recordEvents()
      const worker = startTestWorker(testSchema, {
        tasks: toWorkerTasks([
          sourceTask.implement(
            (_, { signal }) =>
              new Promise((_, reject) =>
                signal.addEventListener("abort", () => reject(new Error("aborted"))),
              ),
            { onFailure: () => {} },
          ),
        ]),
        onEvent: log.onEvent,
      })
      await log.waitFor("start", e => e.claim.id === source.id)
      await worker.stop({ abortAfter: 0 })
      expect((await jobs(testSchema.conn.query)).map(r => [r.task, r.state, r.attempts])).toEqual([
        ["orders.charge", "pending", 0],
      ])
    })

    test("a failure job that spends the maxAttempts of onFailure fails for good, and adds no failure job of its own", async () => {
      await enqueue(testSchema.conn, testSchema.table, { task: "orders.charge", maxAttempts: 1 })
      const log = recordEvents()
      startTestWorker(testSchema, {
        concurrency: 2,
        tasks: toWorkerTasks([
          sourceTask.implement(
            () => {
              throw new Error("card declined")
            },
            {
              onFailure: {
                maxAttempts: 1,
                handler: () => {
                  throw new Error("mail server down")
                },
              },
            },
          ),
        ]),
        onEvent: log.onEvent,
      })
      expect(
        (await log.waitFor("settled", e => e.claim.job.task === "orders.charge:failure")).outcome,
      ).toBe("failed")
      expect(
        (await jobs(testSchema.conn.query)).map(r => [r.task, r.state, r.maxAttempts, r.lastError]),
      ).toEqual([
        ["orders.charge", "failed", 1, "card declined"],
        ["orders.charge:failure", "failed", 1, "mail server down"],
      ])
    })

    test("a failure job whose onFailure throws retries as any job, with its own attempts and error, and its JobFailure keeps the error of the failed job", async () => {
      await enqueue(testSchema.conn, testSchema.table, {
        task: "orders.charge",
        payload: { orderId: "1" },
        maxAttempts: 1,
      })
      const { log } = startFailureWorker(
        () => {
          throw new Error("card declined")
        },
        {
          onFailure: () => {
            throw new Error("mail server down")
          },
        },
      )
      expect(
        (await log.waitFor("settled", e => e.claim.job.task === "orders.charge:failure")).outcome,
      ).toBe("retry")
      const failure = only(await jobs(testSchema.conn.query, sql`task = 'orders.charge:failure'`))
      expect([failure.state, failure.attempts, failure.maxAttempts, failure.lastError]).toEqual([
        "pending",
        1,
        25,
        "mail server down",
      ])
      expect((failure.payload as JobFailure).error.message).toBe("card declined")
    })

    test("a worker that does not implement the task returns the jobs of its id and adds the failure job, which stays pending without an implementation", async () => {
      await enqueue(testSchema.conn, testSchema.table, { task: "orders.charge", maxAttempts: 1 })
      const claim = only(
        await claimPlain(testSchema.conn.query, {
          worker: "restarted",
          tasks: ["orders.charge"],
          limit: 1,
          failureMaxAttempts: { "orders.charge": 4 },
        }),
      )
      const log = recordEvents()
      startTestWorker(testSchema, { id: "restarted", tasks: {}, onEvent: log.onEvent })
      await log.waitFor("recover")
      await sleep(100)
      const failed = only(await jobs(testSchema.conn.query, sql`task = 'orders.charge:failure'`))
      expect([failed.state, failed.maxAttempts]).toEqual(["pending", 4])
      expect(failed.payload).toMatchObject({ id: claim.claimId, reason: "worker-restarted" })
    })
  })
})
