// Workers through the public API on the drivers that the documentation connects: jobs that `createJobs` enqueued run with their payloads and metadata, the errors of handlers decide their retries and reach `onError`, a job that failed for good runs again after `jobs.retry`, a job taken too late to keep its lease fails without running, and `stop` aborts the signal of a running handler, whose job returns without spending its attempt.
import { afterEach, beforeEach, describe, expect, test } from "bun:test"

import { createJobs, defineTask, fixedBackoff, type JobInfo } from "kern-jobs"
import { nodePostgresDriver } from "kern-jobs/db/node-postgres"
import { postgresJsDriver } from "kern-jobs/db/postgres-js"
import {
  defineManifest,
  type Driver,
  HandlerAbort,
  type Manifest,
  RetryError,
  startWorker,
  TerminalError,
  type Worker,
  type WorkerErrorInfo,
} from "kern-jobs/worker"
import pg from "pg"
import postgres from "postgres"

import { createTestSchema, databaseUrl, type TestSchema } from "../testing/database"

const drivers: [string, () => Driver][] = [
  ["postgres.js", () => postgresJsDriver(postgres(databaseUrl(), { max: 3 }))],
  [
    "node-postgres",
    () => nodePostgresDriver(new pg.Pool({ connectionString: databaseUrl(), max: 3 })),
  ],
]

const indexProduct = defineTask("search.indexProduct", { maxAttempts: 3 })
  .withPayload<{ productId: string }>()
  .withOptions(({ productId }) => ({ key: `product:${productId}` }))
const renderPreview = defineTask("documents.renderPreview", {
  backoff: fixedBackoff(100),
}).withPayload<{ documentId: string }>()
const syncAccount = defineTask("accounts.sync").withPayload<{ accountId: string }>()
const importFeed = defineTask("feeds.import", { maxAttempts: 1 }).withPayload<{ feedId: string }>()

let schema: TestSchema
beforeEach(async () => {
  schema = await createTestSchema()
})
afterEach(async () => {
  await schema.dispose()
})

/**
 * Returns the task, state, attempts, error, and seconds until the run time of the jobs of the test schema by id; the seconds are rounded to tens, so that the time that the test takes does not change them.
 */
async function jobRows() {
  const { rows } = await schema.pool.query(
    `select task, state, attempts, last_error, round(extract(epoch from run_at - now()), -1)::int as due
     from "${schema.name}".job order by id`,
  )
  return rows.map(row => [row.task, row.state, row.attempts, row.last_error, row.due])
}

for (const [name, createDriver] of drivers) {
  describe(name, () => {
    let driver: Driver
    let worker: Worker | undefined
    beforeEach(() => {
      driver = createDriver()
    })
    afterEach(async () => {
      await worker?.stop()
      worker = undefined
      await driver.close()
    })

    const createTestJobs = () => createJobs(() => schema.pool, { schemaName: schema.name })

    /** Starts the worker of the test, which polls fast, with the reports of its `onError`. */
    function start(
      manifest: Manifest,
      o: { concurrency?: number; driver?: Driver; lease?: number } = {},
    ) {
      const reports: [unknown, WorkerErrorInfo][] = []
      worker = startWorker(o.driver ?? driver, {
        manifest,
        schemaName: schema.name,
        concurrency: o.concurrency,
        lease: o.lease,
        pollInterval: 0.02,
        onError: (error, info) => reports.push([error, info]),
      })
      return { worker, reports }
    }

    test("runs an enqueued job with its payload and metadata and deletes it", async () => {
      const ran = Promise.withResolvers<[{ productId: string }, JobInfo]>()
      const { id } = await createTestJobs().enqueue(indexProduct, { productId: "42" })
      const { worker, reports } = start(
        defineManifest({
          tasks: [indexProduct.implement((payload, { job }) => ran.resolve([payload, job]))],
        }),
      )
      expect(await ran.promise).toEqual([
        { productId: "42" },
        { id, scope: null, attempt: 1, maxAttempts: 3, key: "product:42", queue: null },
      ])
      await worker.stop()
      expect([await jobRows(), reports]).toEqual([[], []])
    })

    test("retries a failed job after the backoff of its task or the delay of a RetryError, and fails it for good after a TerminalError", async () => {
      const jobs = createTestJobs()
      await jobs.enqueue(indexProduct, { productId: "" })
      await jobs.enqueue(renderPreview, { documentId: "7" })
      await jobs.enqueue(syncAccount, { accountId: "3" })
      const errors = [
        new TerminalError("no product"),
        new Error("the renderer is down"),
        new RetryError("the account is busy", { delay: 30 }),
      ]
      const thrown = errors.map(() => Promise.withResolvers<void>())
      const fail = (index: number) => () => {
        thrown[index]!.resolve()
        throw errors[index]
      }
      const { worker, reports } = start(
        defineManifest({
          tasks: [
            indexProduct.implement(fail(0)),
            renderPreview.implement(fail(1)),
            syncAccount.implement(fail(2)),
          ],
        }),
        { concurrency: 3 },
      )
      await Promise.all(thrown.map(t => t.promise))
      await worker.stop()
      expect(await jobRows()).toEqual([
        ["search.indexProduct", "failed", 1, "no product", 0],
        ["documents.renderPreview", "pending", 1, "the renderer is down", 100],
        ["accounts.sync", "pending", 1, "the account is busy", 30],
      ])
      const byTask = new Map(
        reports.map(([error, info]) => [info.source === "handler" ? info.task : "", error]),
      )
      expect(byTask).toEqual(
        new Map<string, unknown>([
          ["search.indexProduct", errors[0]],
          ["documents.renderPreview", errors[1]],
          ["accounts.sync", errors[2]],
        ]),
      )
    })

    test("a job that failed for good runs again from its first attempt after jobs.retry", async () => {
      const jobs = createTestJobs()
      const { id } = await jobs.enqueue(importFeed, { feedId: "9" })
      const runs: JobInfo[] = []
      const completed = Promise.withResolvers<void>()
      const error = new Error("the feed is down")
      const { worker, reports } = start(
        defineManifest({
          tasks: [
            importFeed.implement((_payload, { job }) => {
              runs.push(job)
              if (runs.length === 1) throw error
              completed.resolve()
            }),
          ],
        }),
      )
      while ((await jobRows())[0]?.[1] !== "failed") await new Promise(r => setTimeout(r, 20))
      expect(await jobs.retry(id)).toEqual({ retried: true })
      await completed.promise
      await worker.stop()
      expect(runs.map(job => [job.id, job.attempt])).toEqual([
        [id, 1],
        [id, 1],
      ])
      expect([await jobRows(), reports.map(([reported]) => reported)]).toEqual([[], [error]])
    })

    test("a job whose claim returns after the deadline of its lease fails without running its handler, and onError gets it", async () => {
      const { id } = await createTestJobs().enqueue(importFeed, { feedId: "9" })
      let slowed = false
      // Returns the result of the first claim of a job after the lease would need a heartbeat.
      const slow: Driver = {
        async execute(statement) {
          const result = await driver.execute(statement)
          if (
            !slowed &&
            result.rows.length > 0 &&
            statement.text.includes("attempts = j.attempts + 1")
          ) {
            slowed = true
            await new Promise(resolve => setTimeout(resolve, 1000))
          }
          return result
        },
        executeScript: text => driver.executeScript(text),
        acquire: () => driver.acquire(),
        close: () => driver.close(),
      }
      let calls = 0
      const { worker, reports } = start(
        defineManifest({ tasks: [importFeed.implement(() => void calls++)] }),
        { driver: slow, lease: 1 },
      )
      while ((await jobRows())[0]?.[1] !== "failed") await new Promise(r => setTimeout(r, 20))
      await worker.stop()
      expect([calls, await jobRows()]).toEqual([
        0,
        [
          [
            "feeds.import",
            "failed",
            1,
            "lease confirmation deadline passed before the handler started",
            0,
          ],
        ],
      ])
      expect(reports.map(([error, info]) => [(error as Error).message, info])).toEqual([
        [
          `Job ${id} of task feeds.import was taken after the deadline of its lease, so its handler did not run, and the attempt counts`,
          { source: "claim" },
        ],
      ])
    })

    test("stop aborts the signal of a running handler with a HandlerAbort, and its job returns without spending its attempt or reaching onError", async () => {
      const aborted = Promise.withResolvers<unknown>()
      await createTestJobs().enqueue(syncAccount, { accountId: "3" })
      const started = Promise.withResolvers<void>()
      const { worker, reports } = start(
        defineManifest({
          tasks: [
            syncAccount.implement(
              (_, { signal }) =>
                new Promise((_resolve, reject) => {
                  signal.addEventListener("abort", () => {
                    aborted.resolve(signal.reason)
                    reject(signal.reason)
                  })
                  started.resolve()
                }),
            ),
          ],
        }),
      )
      await started.promise
      await worker.stop({ abortAfter: 0 })
      const reason = await aborted.promise
      expect(reason).toBeInstanceOf(HandlerAbort)
      expect([(reason as HandlerAbort).reason, reports]).toEqual(["stop", []])
      expect((await jobRows()).map(row => row.slice(0, 4))).toEqual([
        ["accounts.sync", "pending", 0, null],
      ])
    })
  })
}
