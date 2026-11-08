// Worker of an application: runs the jobs of its manifest in its schema, reports the errors of handlers and of the database to `onError`, or to the console without one, runs a reaper by default, becomes ready only once its schema is migrated and never on a pool of one connection, checks its options before it starts anything, and checks the options of its stop before it stops anything.
import { afterEach, beforeEach, describe, expect, spyOn, test } from "bun:test"
import { hostname } from "node:os"

import { type Driver, QueryError } from "kern-db"

import { claimPlain } from "../core/claim"
import { enqueue } from "../core/enqueue"
import { createTestSchema, jobs, sleep, type TestSchema } from "../core/testing"
import { migrateJobs } from "../migration/migrate"
import { fixedBackoff } from "../task/backoff"
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
        },
      ],
    ])
    const rows = await jobs(schema)
    expect(rows.map(row => [row.id, row.state, row.attempts, row.last_error])).toEqual([
      [failed.id, "pending", 1, "the index is down"],
    ])
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
        `Error: The schema "${table.schemaName}" of kern-jobs is not up to date: call migrateJobs of kern-jobs/migrations before the worker starts (pending migrations: 0001_create_job)`,
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
    await schema.admin.executeScript(`drop table ${schema.table.name}`)
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
      [{ manifest: undefined }, TypeError, "manifest.tasks must be an array"],
      [{ manifest: { tasks: [purgeExpiredSessions] } }, TypeError, "A manifest takes"],
      // A task of a wrong setting, built without defineTask.
      [
        { manifest: { tasks: [{ task: { ...purgeExpiredSessions, lease: 1e20 }, handler() {} }] } },
        RangeError,
        "lease of task sessions.purgeExpired",
      ],
      [
        { schemaName: 5 },
        TypeError,
        "schemaName must be a string of 1 to 63 bytes in UTF-8 without U+0000",
      ],
      [{ schemaName: "" }, TypeError, "schemaName"],
      [{ schemaName: null }, TypeError, "schemaName"],
      [{ schemaName: "a".repeat(64) }, TypeError, "schemaName"],
      [{ concurrency: 0 }, RangeError, `concurrency must be an integer from 1 to ${2 ** 31 - 1}`],
      [{ concurrency: 1.5 }, RangeError, "concurrency"],
      [{ concurrency: 2 ** 31 }, RangeError, "concurrency"],
      [{ concurrency: "2" }, RangeError, "concurrency"],
      [{ id: "" }, TypeError, "id must be a string"],
      [{ id: "a\0b" }, TypeError, "id must be a string"],
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

  test("stop throws for wrong options before it stops anything, and a later call joins the stop in progress whatever its options", async () => {
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
    ]
    for (const [options, type, message] of cases) {
      const stop = () => worker.stop(options as StopOptions)
      expect(stop).toThrow(type)
      expect(stop).toThrow(message)
    }
    // The worker goes on: it keeps checking its schema, which fails without a database.
    const before = reports
    while (reports === before) await new Promise(resolve => setTimeout(resolve, 10))
    const stopped = worker.stop({ abortAfter: 0 })
    expect(worker.stop({ abortAfter: -1 })).toBe(stopped)
    await stopped
  })
})
