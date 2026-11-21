// Worker of an application: runs the jobs of its manifest in its schema, through its `runHandler`, reports the errors of handlers and of the database to `onError`, or to the console without one, runs a reaper by default, becomes ready only once its schema is migrated and never on a pool of one connection, checks its options before it starts anything, checks the options of each call of its stop before it stops anything, and gives up a stop with force.
import { afterEach, beforeEach, describe, expect, spyOn, test } from "bun:test"
import { AsyncLocalStorage } from "node:async_hooks"
import { hostname } from "node:os"

import { type Driver, isQueryError, QueryError } from "kern-db"
import { waitForLockedStatement } from "kern-db/testing"

import { claimPlain } from "../core/claim"
import { enqueue } from "../core/enqueue"
import { query } from "../core/statement"
import { createTestSchema, jobs, sleep, type TestSchema } from "../core/testing"
import { migrateJobs } from "../migration/migrate"
import { evaluateSchedule } from "../schedule/evaluate"
import { defineSchedule, toWorkerSchedule } from "../schedule/schedule"
import { fixedBackoff } from "../task/backoff"
import { TerminalError } from "../task/errors"
import { defineManifest } from "../task/manifest"
import { defineTask, type JobInfo } from "../task/task"

import { maxInterval } from "./loop"
import type { StopOptions, Worker } from "./runtime"
import { startWorker, type WorkerErrorInfo, type WorkerOptions } from "./worker"

const indexProduct = defineTask("search.indexProduct", {
  backoff: fixedBackoff(100),
}).withPayload<{ productId: string }>()
const purgeExpiredSessions = defineTask("sessions.purgeExpired")

describe("a running worker", () => {
  let schema: TestSchema
  const workers: Worker[] = []
  beforeEach(async () => {
    schema = await createTestSchema()
  })
  afterEach(async () => {
    await Promise.all(workers.splice(0).map(worker => worker.stop()))
    await schema.dispose()
  })

  /** Starts a worker on the pool of the test schema that polls fast, and stops it after the test. */
  function start(options: Partial<WorkerOptions> & Pick<WorkerOptions, "manifest">): Worker {
    const worker = startWorker(schema.driver, {
      schemaName: schema.name,
      pollInterval: 0.02,
      ...options,
    })
    workers.push(worker)
    return worker
  }

  test("runs the jobs of its manifest in its schema and reports the error of a handler, ignoring what onError throws", async () => {
    const failure = new Error("the index is down")
    const purged = Promise.withResolvers<unknown>()
    const reported = Promise.withResolvers<void>()
    const reports: [unknown, WorkerErrorInfo][] = []
    const failed = await enqueue(schema.admin, schema.table, {
      task: "search.indexProduct",
      payload: { productId: "42" },
      key: "product:42",
    })
    await enqueue(schema.admin, schema.table, { task: "sessions.purgeExpired" })
    const worker = start({
      id: "indexer",
      manifest: defineManifest({
        tasks: [
          indexProduct.implement(() => {
            throw failure
          }),
          purgeExpiredSessions.implement(payload => purged.resolve(payload)),
        ],
      }),
      concurrency: 2,
      reaper: false,
      onError(error, info) {
        reports.push([error, info])
        reported.resolve()
        throw new Error("the report failed")
      },
    })
    expect(worker.id).toBe("indexer")
    expect(await purged.promise).toBeUndefined()
    await reported.promise
    // The stop waits for the settlements of both jobs.
    await worker.stop()
    expect(reports).toEqual([
      [
        failure,
        {
          source: "handler",
          task: "search.indexProduct",
          job: {
            id: failed.id,
            scope: null,
            attempt: 1,
            maxAttempts: 25,
            key: "product:42",
            queue: null,
          },
          aborted: null,
          decision: "retry",
        },
      ],
    ])
    const rows = await jobs(schema)
    expect(rows.map(row => [row.id, row.state, row.attempts, row.last_error])).toEqual([
      [failed.id, "pending", 1, "the index is down"],
    ])
  })

  test("settles a job by the decision that it reports, also when onError changes the class of the error", async () => {
    const terminal = await enqueue(schema.admin, schema.table, { task: "search.indexProduct" })
    const retried = await enqueue(schema.admin, schema.table, { task: "sessions.purgeExpired" })
    const decisions = new Map<string, string>()
    const reported = Promise.withResolvers<void>()
    const worker = start({
      manifest: defineManifest({
        tasks: [
          indexProduct.implement(() => {
            throw new TerminalError("no product")
          }),
          purgeExpiredSessions.implement(() => {
            throw new Error("the database is down")
          }),
        ],
      }),
      concurrency: 2,
      reaper: false,
      onError(error, info) {
        if (info.source !== "handler") return
        decisions.set(info.job.id, info.decision)
        // Makes the terminal error an ordinary one and the ordinary error a terminal one.
        const prototype = error instanceof TerminalError ? Error.prototype : TerminalError.prototype
        Object.setPrototypeOf(error, prototype)
        if (decisions.size === 2) reported.resolve()
      },
    })
    await reported.promise
    await worker.stop()
    expect([...decisions]).toEqual(
      expect.arrayContaining([
        [terminal.id, "terminal"],
        [retried.id, "retry"],
      ]),
    )
    expect((await jobs(schema)).map(row => [row.id, row.state])).toEqual([
      [terminal.id, "failed"],
      [retried.id, "pending"],
    ])
  })

  test("runs each handler through runHandler, such as in the context of the scope of its job, and completes a job whose handler runHandler skips", async () => {
    const tenant = new AsyncLocalStorage<string>()
    const indexed = Promise.withResolvers<string | undefined>()
    const indexTenantProduct = defineTask("search.indexProduct", { scoped: true })
    await enqueue(schema.admin, schema.table, { task: "search.indexProduct", scope: "deleted" })
    await enqueue(schema.admin, schema.table, { task: "search.indexProduct", scope: "acme" })
    const worker = start({
      manifest: defineManifest({
        tasks: [indexTenantProduct.implement(() => indexed.resolve(tenant.getStore()))],
      }),
      reaper: false,
      runHandler: ({ job }, run) =>
        job.scope === "deleted" ? undefined : tenant.run(`tenant ${job.scope}`, run),
      onError: error => indexed.reject(error),
    })
    expect(await indexed.promise).toBe("tenant acme")
    await worker.stop()
    expect(await jobs(schema)).toEqual([])
  })

  test("without onError, writes an error to the console", async () => {
    const failure = new Error("the index is down")
    const thrown = Promise.withResolvers<void>()
    const job = await enqueue(schema.admin, schema.table, {
      task: "search.indexProduct",
      payload: { productId: "42" },
    })
    const error = spyOn(console, "error").mockImplementation(() => {})
    try {
      const worker = start({
        manifest: defineManifest({
          tasks: [
            indexProduct.implement(() => {
              thrown.resolve()
              throw failure
            }),
          ],
        }),
        reaper: false,
      })
      await thrown.promise
      await worker.stop()
      expect(error.mock.calls).toEqual([
        [`kern-jobs: handler error, task search.indexProduct, job ${job.id}:`, failure],
      ])
    } finally {
      error.mockRestore()
    }
  })

  test("by default, takes a name of its host and process, and its reaper returns a job of an expired lease, which it runs", async () => {
    const hourAgo = new Date(Date.now() - 3600_000)
    await enqueue(schema.admin, schema.table, { task: "sessions.purgeExpired", now: hourAgo })
    await claimPlain(schema.admin, schema.table, {
      worker: "dead",
      tasks: ["sessions.purgeExpired"],
      defaultLease: 10,
      limit: 1,
      now: hourAgo,
    })
    const ran = Promise.withResolvers<JobInfo>()
    const worker = start({
      manifest: defineManifest({
        tasks: [purgeExpiredSessions.implement((_, { job }) => ran.resolve(job))],
      }),
    })
    expect(worker.id).toStartWith(`${hostname()}:${process.pid}:`)
    expect((await ran.promise).attempt).toBe(2)
    await worker.stop()
    expect(await jobs(schema)).toEqual([])
  })

  test("without onError, writes a schedule error to the console with the schedule ID", async () => {
    const failure = new Error("the options are unavailable")
    const schedule = defineSchedule(
      defineTask("reports.daily").withOptions(() => {
        throw failure
      }),
      { id: "analytics.daily", cron: { minute: "*" } },
    )
    const logged = Promise.withResolvers<void>()
    const error = spyOn(console, "error").mockImplementation(() => logged.resolve())
    try {
      const worker = start({ manifest: defineManifest({ tasks: [], schedules: [schedule] }) })
      await logged.promise
      await worker.stop()
      expect(error.mock.calls[0]).toEqual([
        "kern-jobs: schedule error, schedule analytics.daily:",
        failure,
      ])
    } finally {
      error.mockRestore()
    }
  })

  test("runs a recovered occurrence once across three workers, also after all three stop and one restarts", async () => {
    // Pick a minute an hour ago so the next daily occurrence cannot arrive during the test.
    const [clock] = await query(schema.admin, "select extract(epoch from now()) as seconds")
    const at = new Date((Math.floor(Number(clock!.seconds) / 60) - 60) * 60000)
    const schedule = defineSchedule(indexProduct, {
      cron: { minute: at.getUTCMinutes(), hour: at.getUTCHours() },
      catchUp: { within: 7200 },
      payload: { productId: "scheduled" },
    })
    await evaluateSchedule(schema.database, schema.table, toWorkerSchedule(schedule), {
      now: new Date(at.getTime() - 60000),
    })
    const handled = Promise.withResolvers<void>()
    const products: string[] = []
    const manifest = defineManifest({
      tasks: [
        indexProduct.implement(({ productId }) => {
          products.push(productId)
          handled.resolve()
        }),
      ],
      schedules: [schedule],
    })
    const launch = () => start({ manifest, onError: error => handled.reject(error) })
    const replicas = [launch(), launch(), launch()]
    await Promise.all([handled.promise, ...replicas.map(w => w.ready)])
    await Promise.all(replicas.map(w => w.stop()))
    expect(await jobs(schema)).toEqual([])
    const restarted = launch()
    await restarted.ready
    await restarted.stop()
    expect(await jobs(schema)).toEqual([])
    expect(products).toEqual(["scheduled"])
  })

  test("enqueues from a manifest without tasks for a worker with another manifest", async () => {
    const handled = Promise.withResolvers<string>()
    const schedule = defineSchedule(indexProduct, {
      cron: { minute: "*" },
      payload: { productId: "separate" },
    })
    start({
      manifest: defineManifest({ tasks: [], schedules: [schedule] }),
      onError: error => handled.reject(error),
    })
    start({
      manifest: defineManifest({
        tasks: [indexProduct.implement(({ productId }) => handled.resolve(productId))],
      }),
      onError: error => handled.reject(error),
    })
    expect(await handled.promise).toBe("separate")
  })

  test("reports a schedule blocked by a producer's pending key and continues to the next schedule", async () => {
    const blocked = defineSchedule(indexProduct, {
      cron: { minute: "*" },
      payload: { productId: "scheduled" },
      enqueue: { key: "shared" },
    })
    // Register before either replica starts, so the second finds an existing schedule row.
    await query(
      schema.admin,
      `insert into "${schema.name}".schedule (id, evaluated_through) values ($1::text, 'epoch')`,
      [blocked.id],
    )
    const independent = defineSchedule(purgeExpiredSessions, { cron: { minute: "*" } })
    const reported = Promise.withResolvers<WorkerErrorInfo>()
    const handled = Promise.withResolvers<void>()
    const manifest = defineManifest({
      tasks: [
        indexProduct.implement(() => {}),
        purgeExpiredSessions.implement(() => handled.resolve()),
      ],
      schedules: [blocked, independent],
    })
    const launch = () =>
      start({
        manifest,
        onError(error, info) {
          if (isQueryError(error, "lock_not_available")) reported.resolve(info)
          else reported.reject(error)
        },
      })

    const producer = await schema.connect()
    await query(producer, "begin")
    try {
      await enqueue(producer, schema.table, { task: indexProduct.name, key: "shared" })
      launch()
      await waitForLockedStatement(schema.admin, `insert into ${schema.table.name} as j`)
      launch()
      // Keep the producer transaction open until the error and the unrelated delivery are both observed.
      const deadline = AbortSignal.timeout(4000)
      const expired = new Promise<never>((_, reject) => {
        deadline.addEventListener("abort", () =>
          reject(new Error("The schedules stalled behind the producer")),
        )
      })
      const [error] = await Promise.race([
        Promise.all([reported.promise, handled.promise]),
        expired,
      ])
      expect(error).toEqual({ source: "schedule", id: blocked.id })
    } finally {
      await query(producer, "rollback")
    }
  })

  test("takes no jobs in a schema that migrateJobs has not created, reports it, and becomes ready once the schema is migrated", async () => {
    const table = schema.unmigratedTable("late")
    const reports: [unknown, WorkerErrorInfo][] = []
    const reportedTwice = Promise.withResolvers<void>()
    const ran = Promise.withResolvers<void>()
    try {
      const worker = start({
        manifest: defineManifest({ tasks: [purgeExpiredSessions.implement(() => ran.resolve())] }),
        schemaName: table.schemaName,
        onError(error, info) {
          if (reports.push([error, info]) === 2) reportedTwice.resolve()
        },
      })
      await reportedTwice.promise
      expect(worker.state).toBe("starting")
      // Neither a claim nor the reaper has run in the schema, which would fail without the table.
      expect(reports.map(([, info]) => info)).toEqual([{ source: "schema" }, { source: "schema" }])
      expect(String(reports[0]![0])).toBe(
        `Error: The schema "${table.schemaName}" of kern-jobs is not up to date: call migrateJobs of kern-jobs/migrations before the worker starts (pending migrations: 0001_create_job, 0002_schedule, 0003_job_summary, 0004_failure_jobs)`,
      )

      await migrateJobs(schema.driver, { schemaName: table.schemaName })
      await worker.ready
      expect(worker.state).toBe("ready")
      await enqueue(schema.admin, table, { task: "sessions.purgeExpired" })
      await ran.promise
    } finally {
      await Promise.all(workers.splice(0).map(worker => worker.stop()))
    }
  })

  test("on a pool of one connection, which its heartbeat keeps, never becomes ready and takes no jobs, and stops only at its timeout", async () => {
    await enqueue(schema.admin, schema.table, { task: "sessions.purgeExpired" })
    const reports: unknown[] = []
    let ran = false
    const worker = startWorker(schema.createDriver({ max: 1 }), {
      manifest: defineManifest({
        tasks: [purgeExpiredSessions.implement(() => void (ran = true))],
      }),
      schemaName: schema.name,
      pollInterval: 0.02,
      onError: error => reports.push(error),
    })
    let ready = false
    void worker.ready.then(() => (ready = true)).catch(() => {})
    await sleep(300)
    expect([worker.state, ready, ran, reports]).toEqual(["starting", false, false, []])
    // The check of the schema waits for the connection that the heartbeat releases only at the end of the stop.
    await expect(worker.stop({ timeout: 0.1 })).rejects.toThrow(
      "The worker did not stop within 0.1 s",
    )
    expect(worker.state).toBe("stopped")
    await expect(worker.ready).rejects.toThrow("The worker stopped before it was ready")
    expect((await jobs(schema)).map(job => job.state)).toEqual(["pending"])
  })

  test("stop with force gives up at once and keeps its promise before the abort, so a stop called from the abort of a handler returns it", async () => {
    await enqueue(schema.admin, schema.table, { task: "sessions.purgeExpired" })
    const started = Promise.withResolvers<void>()
    const release = Promise.withResolvers<void>()
    let fromAbort: Promise<void> | undefined
    const worker: Worker = startWorker(schema.driver, {
      manifest: defineManifest({
        tasks: [
          purgeExpiredSessions.implement((_payload, { signal }) => {
            // A block body, since Bun reports the rejection of a promise that a listener returns as unhandled.
            signal.addEventListener("abort", () => {
              fromAbort = worker.stop()
            })
            started.resolve()
            // Ignores the abort.
            return release.promise
          }),
        ],
      }),
      schemaName: schema.name,
      pollInterval: 0.02,
      reaper: false,
    })
    await started.promise
    // A valid deadline beside force changes nothing.
    const stopped = worker.stop({ force: true, timeout: 10 })
    expect(fromAbort).toBe(stopped)
    await expect(stopped).rejects.toThrow(
      "The worker stop was forced; jobs still running or settling: 1",
    )
    release.resolve()
  })

  test("a later stop without force changes nothing, and one with force gives up the stop in progress, which rejects before its handler ends", async () => {
    await enqueue(schema.admin, schema.table, { task: "sessions.purgeExpired" })
    const started = Promise.withResolvers<AbortSignal>()
    const release = Promise.withResolvers<void>()
    const worker = startWorker(schema.driver, {
      manifest: defineManifest({
        tasks: [
          purgeExpiredSessions.implement((_payload, { signal }) => {
            started.resolve(signal)
            return release.promise
          }),
        ],
      }),
      schemaName: schema.name,
      pollInterval: 0.02,
      reaper: false,
    })
    const signal = await started.promise
    const stopped = worker.stop({ abortAfter: 60 })
    let settled = false
    stopped.catch(() => {}).finally(() => (settled = true))
    expect(worker.stop({ abortAfter: 0, timeout: 0 })).toBe(stopped)
    await sleep(100)
    expect([signal.aborted, settled]).toEqual([false, false])
    expect(worker.stop({ force: true })).toBe(stopped)
    expect(signal.aborted).toBe(true)
    await expect(stopped).rejects.toThrow("The worker stop was forced")
    release.resolve()
  })

  test("reports a failure of the database, such as a claim after the job table is dropped, and goes on", async () => {
    const claims: unknown[] = []
    const reportedTwice = Promise.withResolvers<void>()
    const ran = Promise.withResolvers<void>()
    await enqueue(schema.admin, schema.table, { task: "sessions.purgeExpired" })
    start({
      manifest: defineManifest({ tasks: [purgeExpiredSessions.implement(() => ran.resolve())] }),
      reaper: false,
      onError(error, info) {
        if (info.source === "claim" && claims.push(error) === 2) reportedTwice.resolve()
      },
    })
    // The worker has checked its schema once it runs a job.
    await ran.promise
    await schema.admin.executeScript(`drop table ${schema.table.name} cascade`)
    await reportedTwice.promise
    expect(claims.map(error => error instanceof QueryError)).toEqual([true, true])
  })
})

describe("options", () => {
  const manifest = defineManifest({ tasks: [purgeExpiredSessions.implement(() => {})] })

  test("rejects a wrong driver or option before the worker starts anything", async () => {
    let calls = 0
    const call = () => {
      calls++
      return Promise.reject(new Error("the worker started"))
    }
    const driver: Driver = { execute: call, executeScript: call, acquire: call, close: call }
    // A client instead of its driver.
    expect(() => startWorker({ query: call } as unknown as Driver, { manifest })).toThrow(
      new TypeError(
        "driver must be a driver of kern-db, such as postgresJsDriver(sql) of kern-jobs/db/postgres-js",
      ),
    )
    const cases: [Record<string, unknown>, ErrorConstructor, string][] = [
      [
        { manifest: undefined },
        TypeError,
        "manifest must be an object, which defineManifest returns",
      ],
      [{ manifest: {} }, TypeError, "manifest.tasks must be an array"],
      [{ manifest: { tasks: [purgeExpiredSessions] } }, TypeError, "A manifest takes"],
      // A task of a wrong setting, built without defineTask.
      [
        { manifest: { tasks: [{ task: { ...purgeExpiredSessions, lease: 1e20 }, handler() {} }] } },
        RangeError,
        "lease of task sessions.purgeExpired",
      ],
      [{ schemaName: 5 }, TypeError, "schemaName must be"],
      [{ schemaName: "" }, TypeError, "schemaName"],
      [{ schemaName: null }, TypeError, "schemaName"],
      [{ schemaName: "a".repeat(64) }, TypeError, "schemaName"],
      [{ concurrency: 0 }, RangeError, `concurrency must be an integer from 1 to ${2 ** 31 - 1}`],
      [{ concurrency: 1.5 }, RangeError, "concurrency"],
      [{ concurrency: 2 ** 31 }, RangeError, "concurrency"],
      [{ concurrency: "2" }, RangeError, "concurrency"],
      [{ id: "" }, TypeError, "id must be a string"],
      [{ id: "a\0b" }, TypeError, "id must be a string"],
      [{ id: "a\ud800" }, TypeError, "id must be a string"],
      [{ lease: 0 }, RangeError, "lease must be a number of seconds above 0 up to 1e12"],
      [{ lease: 1e12 + 1 }, RangeError, "lease"],
      [{ pollInterval: 0 }, RangeError, "pollInterval must be a number of seconds above 0"],
      [{ pollInterval: NaN }, RangeError, "pollInterval"],
      // A timer of a delay above 2^31 - 1 milliseconds fires at once.
      [{ pollInterval: maxInterval + 1 }, RangeError, "pollInterval"],
      [{ reaper: true }, TypeError, "reaper must be false or an options object"],
      [{ reaper: null }, TypeError, "reaper"],
      [{ reaper: { interval: 0 } }, RangeError, "reaper.interval"],
      [{ reaper: { interval: maxInterval + 1 } }, RangeError, "reaper.interval"],
      [{ reaper: { limit: 0 } }, RangeError, "reaper.limit must be an integer"],
      [{ runHandler: "wrap" }, TypeError, "runHandler must be a function"],
      [{ onError: "log" }, TypeError, "onError must be a function"],
    ]
    for (const [option, type, message] of cases) {
      const start = () => startWorker(driver, { manifest, ...option } as WorkerOptions)
      expect(start).toThrow(type)
      expect(start).toThrow(message)
    }
    await new Promise(resolve => setTimeout(resolve, 10))
    expect(calls).toBe(0)
  })

  test("stop throws for wrong options before it stops anything, also on a later call, and a later call without force joins the stop in progress whatever its deadlines", async () => {
    const call = () => Promise.reject(new Error("no database"))
    const driver: Driver = { execute: call, executeScript: call, acquire: call, close: call }
    let reports = 0
    const worker = startWorker(driver, { manifest, pollInterval: 0.02, onError: () => reports++ })
    const cases: [unknown, ErrorConstructor, string][] = [
      ["5", TypeError, "The options of stop must be an object"],
      [null, TypeError, "The options of stop must be an object"],
      [
        { abortAfter: -1 },
        RangeError,
        `abortAfter must be a number of seconds from 0 up to ${maxInterval}, got -1`,
      ],
      [{ abortAfter: "1" }, RangeError, "abortAfter"],
      [{ timeout: NaN }, RangeError, "timeout"],
      [{ timeout: maxInterval + 1 }, RangeError, "timeout"],
      [{ force: "yes" }, TypeError, "force must be a boolean"],
      [{ force: true, timeout: -1 }, RangeError, "timeout"],
    ]
    const throwsForEach = () => {
      for (const [options, type, message] of cases) {
        const stop = () => worker.stop(options as StopOptions)
        expect(stop).toThrow(type)
        expect(stop).toThrow(message)
      }
    }
    throwsForEach()
    // The worker goes on: it keeps checking its schema, which fails without a database.
    const before = reports
    while (reports === before) await new Promise(resolve => setTimeout(resolve, 10))
    expect(worker.state).toBe("starting")
    const stopped = worker.stop({ abortAfter: 60 })
    throwsForEach()
    expect(worker.stop({ abortAfter: 0, timeout: 0 })).toBe(stopped)
    await stopped
    expect(worker.stop({ force: true })).toBe(stopped)
  })

  test("stop acts on the options it checked, also when a getter returns another value when read again", async () => {
    const call = () => Promise.reject(new Error("no database"))
    const driver: Driver = { execute: call, executeScript: call, acquire: call, close: call }
    const worker = startWorker(driver, { manifest, onError: () => {} })
    let reads = 0
    const options = {
      get force() {
        return reads++ === 0 ? false : "yes"
      },
    }
    await worker.stop(options as StopOptions)
    expect(reads).toBe(1)
  })
})
