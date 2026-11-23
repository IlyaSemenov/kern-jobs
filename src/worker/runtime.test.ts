// Worker runtime: the drivers it runs on, a failed release of a connection, the check of its schema before it claims and reaps, its state and readiness, slots taken until settlement, a long settlement beside the heartbeat of other jobs, zombies of reclaimed jobs, lease origins of claims, heartbeats one at a time, claiming only with a working heartbeat, aborts of lost and unconfirmed leases, the jobs of a previous worker of an id and a worker whose id a running one holds, retries, terminal and unprintable errors, and stopping.
import { afterEach, beforeEach, describe, expect, test } from "bun:test"
import * as timers from "node:timers/promises"

import { type Connection, type Driver, QueryError } from "kern-orm/db"
import { nodePostgresDriver } from "kern-orm/db/node-postgres"
import { waitForLockedStatement } from "kern-orm/testing"
import pg from "pg"

import { defineSchedule, toWorkerSchedule } from "../schedule/schedule"
import { enqueue } from "../sql/enqueue"
import { toStatementLabel } from "../sql/statement"
import { RetryError, TerminalError } from "../task/errors"
import { defineTask } from "../task/task"
import {
  createTestDriver,
  createTestSchema,
  databaseUrl,
  getStatementLabel,
  holdHook,
  jobs,
  only,
  sleep,
  type TestSchema,
} from "../test-harness/database"

import { claimPlain } from "./db/claim"
import { query } from "./db/query"
import { type Proxy, proxyTo } from "./faults/proxy"
import { HandlerAbort } from "./heartbeat"
import type { WorkerTask } from "./runtime"
import {
  createAcquireOnlyDriver,
  reapNow,
  recordEvents,
  startTestWorker,
  stopTestWorkers,
  terminateBackends,
  testTimings,
} from "./test-harness"

let schema: TestSchema
const proxies: Proxy[] = []
beforeEach(async () => {
  schema = await createTestSchema()
})
afterEach(async () => {
  for (const open of openAll.splice(0)) open()
  await stopTestWorkers()
  await Promise.all(proxies.splice(0).map(p => p.close()))
  await schema.dispose()
})

const far = () => new Date(Date.now() + 3600_000)
const { lease, heartbeatInterval: interval } = testTimings

/** Openers of the gates of every test, which the teardown opens so that the workers can stop. */
const openAll: (() => void)[] = []

/** Handlers that wait until the test opens their gate by claim id, ignoring the abort unless `honorAbort`. */
function createGates(o: { honorAbort?: boolean } = {}) {
  const gates = new Map<string, PromiseWithResolvers<void>>()
  const gate = (claimId: string) => {
    let g = gates.get(claimId)
    if (!g) gates.set(claimId, (g = Promise.withResolvers()))
    return g
  }
  openAll.push(() => {
    for (const g of gates.values()) g.resolve()
  })
  const task: WorkerTask = {
    async handler(job, { signal }) {
      const g = gate(job.claim_id!)
      if (o.honorAbort) {
        await Promise.race([
          g.promise,
          new Promise<never>((_, reject) =>
            signal.addEventListener("abort", () => reject(signal.reason)),
          ),
        ])
      } else await g.promise
    },
  }
  return { task, open: (claimId: string) => gate(claimId).resolve() }
}

/**
 * Heartbeat driver over `base` whose connections record the text of each statement in `texts` when it is sent and in `answered` when it succeeds, and reject its `heartbeat:ping` statement with a `QueryError` at once while `failing` is set.
 * `released` resolves once a connection is released.
 */
function createWatchedHeartbeat(base: Driver) {
  const released = Promise.withResolvers<void>()
  const watched = {
    texts: [] as string[],
    answered: [] as string[],
    failing: false,
    driver: base,
    released: released.promise,
  }
  watched.driver = {
    ...base,
    async acquire(): Promise<Connection> {
      const connection = await base.acquire()
      return {
        execute(statement) {
          watched.texts.push(statement.text)
          if (watched.failing && statement.text.includes(toStatementLabel("heartbeat:ping")))
            return Promise.reject(new QueryError("canceling statement", { code: "57014" }))
          return connection.execute(statement).then(result => {
            watched.answered.push(statement.text)
            return result
          })
        },
        executeScript: text => connection.executeScript(text),
        release: async () => {
          await connection.release()
          released.resolve()
        },
      }
    },
  }
  return watched
}

/** Driver of the heartbeat through a proxy. */
async function proxiedHeartbeat() {
  const { proxy, url } = await proxyTo(databaseUrl())
  proxies.push(proxy)
  return { proxy, source: createTestDriver(url, schema.name, { max: 1 }) }
}

describe("drivers", () => {
  test("a worker completes and retries jobs on a driver of node-postgres", async () => {
    const driver = nodePostgresDriver(new pg.Pool({ connectionString: databaseUrl(), max: 3 }))
    try {
      const completed = await enqueue(schema.admin, schema.table, { task: "complete" })
      const retried = await enqueue(schema.admin, schema.table, { task: "retry" })
      const log = recordEvents()
      startTestWorker(schema, {
        driver,
        concurrency: 2,
        tasks: {
          complete: { handler: () => {} },
          retry: {
            handler: () => {
              throw new Error("boom")
            },
            backoff: () => 100,
          },
        },
        onEvent: log.onEvent,
      })
      const outcomes = await Promise.all(
        [completed, retried].map(j => log.waitFor("settled", e => e.claim.id === j.id)),
      )
      expect(outcomes.map(e => e.outcome)).toEqual(["completed", "retry"])
      const rows = await jobs(schema)
      expect(rows.map(row => [row.id, row.state, row.attempts, row.last_error])).toEqual([
        [retried.id, "pending", 1, "boom"],
      ])
    } finally {
      // The pool waits for the heartbeat connection that the worker holds until it stops.
      await stopTestWorkers()
      await driver.close()
    }
  })
  test.each(["postgres.js", "node-postgres"])(
    "a failed release of the connection of fail on %s is reported without changing the outcome of the settlement",
    async client => {
      const pool =
        client === "postgres.js"
          ? schema.driver
          : nodePostgresDriver(new pg.Pool({ connectionString: databaseUrl(), max: 2 }))
      const job = await enqueue(schema.admin, schema.table, { task: "retry" })
      const releaseError = new Error("release failed")
      const driver: Driver = {
        ...pool,
        async acquire() {
          const connection = await pool.acquire()
          return {
            ...connection,
            async release() {
              await connection.release()
              throw releaseError
            },
          }
        },
      }
      const log = recordEvents()
      try {
        startTestWorker(schema, {
          driver,
          heartbeatSource: schema.driver,
          tasks: {
            retry: {
              handler: () => {
                throw new Error("boom")
              },
              backoff: () => 100,
            },
          },
          onEvent: log.onEvent,
        })
        const settled = await log.waitFor("settled", e => e.claim.id === job.id)
        expect([settled.outcome, log.all("release-error").map(e => e.error)]).toEqual([
          "retry",
          [releaseError],
        ])
        // The driver released the connection before the failure, so the pool gives it to the next statement.
        expect(await query(pool, "select 1 as one")).toEqual([{ one: "1" }])
      } finally {
        await stopTestWorkers()
        if (pool !== schema.driver) await pool.close()
      }
    },
  )
})

describe("schema check", () => {
  test("a worker claims and reaps nothing until the check of its schema passes, and checks again after the poll interval", async () => {
    // A job of a dead worker whose lease expired: the reaper returns it, and the worker then claims it.
    const hourAgo = new Date(Date.now() - 3600_000)
    const job = await enqueue(schema.admin, schema.table, { task: "t", now: hourAgo })
    await claimPlain(schema.admin, schema.table, {
      worker: "dead",
      tasks: ["t"],
      defaultLease: 10,
      limit: 1,
      now: hourAgo,
    })
    const log = recordEvents()
    const failure = new Error("the schema is behind")
    let checks = 0
    let statements = 0
    let statementsBeforePass: number | undefined
    startTestWorker(schema, {
      // Claims and the reaper run on the pool, and the heartbeat on a connection that it reserves.
      driver: {
        ...schema.driver,
        execute(statement) {
          statements++
          return schema.driver.execute(statement)
        },
      },
      tasks: { t: { handler: () => {} } },
      reaper: { interval: 0.02, limit: 10 },
      async checkSchema() {
        if (++checks <= 2) throw failure
        statementsBeforePass = statements
      },
      onEvent: log.onEvent,
    })
    const settled = await log.waitFor("settled", e => e.claim.id === job.id)
    expect([settled.outcome, settled.claim.job.attempts]).toEqual(["completed", 2])
    expect(log.all("schema-error").map(e => e.error)).toEqual([failure, failure])
    expect([checks, statementsBeforePass]).toEqual([3, 0])
  })

  test("a stop during the check of the schema waits for it, and the worker then neither claims nor reaps", async () => {
    // A job of a dead worker whose lease expired, which a reaper would return, and a pending job to claim.
    const hourAgo = new Date(Date.now() - 3600_000)
    await enqueue(schema.admin, schema.table, { task: "t", now: hourAgo })
    await claimPlain(schema.admin, schema.table, {
      worker: "dead",
      tasks: ["t"],
      defaultLease: 10,
      limit: 1,
      now: hourAgo,
    })
    await enqueue(schema.admin, schema.table, { task: "t" })
    const log = recordEvents()
    const checking = Promise.withResolvers<void>()
    const checked = Promise.withResolvers<void>()
    openAll.push(() => checked.resolve())
    let statements = 0
    const heartbeatSource = schema.createDriver({ max: 1 })
    const worker = startTestWorker(schema, {
      driver: {
        ...schema.driver,
        execute(statement) {
          statements++
          return schema.driver.execute(statement)
        },
      },
      heartbeatSource,
      tasks: { t: { handler: () => {} } },
      reaper: { interval: 0.02, limit: 10 },
      async checkSchema() {
        checking.resolve()
        await checked.promise
      },
      onEvent: log.onEvent,
    })
    await checking.promise
    let stopped = false
    const stop = worker.stop().then(() => (stopped = true))
    await sleep(20)
    expect(stopped).toBe(false)
    checked.resolve()
    await stop
    expect([statements, log.all("reap"), log.all("start")]).toEqual([0, [], []])
    expect((await jobs(schema)).map(job => job.state)).toEqual(["active", "pending"])
    // The pool of one connection gives it once the heartbeat has released it.
    expect(await query(heartbeatSource, "select 1 as one")).toEqual([{ one: "1" }])
  })
})

describe("state", () => {
  test("a worker is starting until it has checked its schema and its first successful claim cycle has returned, then ready, stopping, and stopped", async () => {
    const release = await holdHook(schema.admin, "claim_plain")
    const checked = Promise.withResolvers<void>()
    openAll.push(() => checked.resolve())
    let resolved = false
    const worker = startTestWorker(schema, {
      driver: schema.createDriver({ hooks: ["claim_plain"] }),
      heartbeatSource: schema.createDriver({ max: 1 }),
      tasks: { t: { handler: () => {} } },
      checkSchema: () => checked.promise,
    })
    void worker.ready.then(() => (resolved = true))
    await sleep(50)
    const whileChecking = worker.state
    checked.resolve()
    await waitForLockedStatement(schema.admin, "kern_test_hook('claim_plain')")
    const whileClaiming = [worker.state, resolved]
    await release()
    await worker.ready
    expect([whileChecking, whileClaiming, worker.state]).toEqual([
      "starting",
      ["starting", false],
      "ready",
    ])
    const stop = worker.stop()
    expect(worker.state).toBe("stopping")
    await stop
    expect(worker.state).toBe("stopped")
  })

  test("a worker without tasks becomes ready after one statement on its pool and sends no claim statement", async () => {
    const statements: string[] = []
    const worker = startTestWorker(schema, {
      driver: {
        ...schema.driver,
        execute(statement) {
          statements.push(statement.text)
          return schema.driver.execute(statement)
        },
      },
      tasks: {},
    })
    await worker.ready
    // Claim cycles would run every poll interval.
    await sleep(100)
    expect(statements.map(getStatementLabel)).toEqual([toStatementLabel("worker:ping")])
  })

  test.each(["without tasks", "whose only slot runs the job it claimed"])(
    "a worker %s whose heartbeat fails during its statement on the pool becomes ready once the heartbeat recovers",
    async name => {
      const withJob = name !== "without tasks"
      if (withJob) await enqueue(schema.admin, schema.table, { task: "t" })
      const { proxy, source } = await proxiedHeartbeat()
      const log = recordEvents()
      const returned = Promise.withResolvers<void>()
      const answer = Promise.withResolvers<void>()
      const handlerGate = Promise.withResolvers<void>()
      openAll.push(answer.resolve, handlerGate.resolve)
      let statements = 0
      let resolved = false
      const worker = startTestWorker(schema, {
        driver: {
          ...schema.driver,
          async execute(statement) {
            const result = await schema.driver.execute(statement)
            if (++statements === 1) {
              returned.resolve()
              await answer.promise
            }
            return result
          },
        },
        heartbeatSource: source,
        tasks: withJob ? { t: { handler: () => handlerGate.promise } } : {},
        onEvent: log.onEvent,
      })
      void worker.ready.then(() => (resolved = true))
      await returned.promise
      proxy.refuse = true
      proxy.cut()
      await log.waitFor("heartbeat-error")
      answer.resolve()
      // The promise chain of the statement settles before the next macrotask.
      await timers.setImmediate()
      expect([resolved, worker.state]).toEqual([false, "starting"])
      proxy.refuse = false
      await worker.ready
      expect(statements).toBe(1)
    },
  )

  test("ready rejects once stop begins before the worker is ready", async () => {
    const worker = startTestWorker(schema, {
      tasks: { t: { handler: () => {} } },
      checkSchema: () => Promise.reject(new Error("the schema is behind")),
    })
    await sleep(50)
    expect(worker.state).toBe("starting")
    void worker.stop()
    await expect(worker.ready).rejects.toThrow("The worker stopped before it was ready")
  })

  test("a worker is unhealthy while its heartbeat connection is broken, and ready once a heartbeat succeeds again", async () => {
    const { proxy, source } = await proxiedHeartbeat()
    const log = recordEvents()
    const worker = startTestWorker(schema, {
      heartbeatSource: source,
      tasks: { t: { handler: () => {} } },
      onEvent: log.onEvent,
    })
    await worker.ready
    proxy.refuse = true
    proxy.cut()
    await log.waitFor("heartbeat-error")
    const broken = worker.state
    proxy.refuse = false
    await log.waitFor("heartbeat-recovered")
    expect([broken, worker.state]).toEqual(["unhealthy", "ready"])
  })
})

describe("slots", () => {
  test("a slot stays taken until its job is settled, and a long settlement keeps the leases of other jobs", async () => {
    const a = await enqueue(schema.admin, schema.table, { task: "fail", key: "k" })
    const c = await enqueue(schema.admin, schema.table, { task: "wait" })
    const failGate = Promise.withResolvers<void>()
    const gates = createGates()
    const log = recordEvents()
    startTestWorker(schema, {
      concurrency: 2,
      tasks: {
        fail: {
          async handler() {
            await failGate.promise
            throw new Error("boom")
          },
        },
        wait: gates.task,
      },
      onEvent: log.onEvent,
    })
    await log.waitFor("start", e => e.claim.id === a.id)
    // The pending job of the key, held by an open producer transaction, makes the fail of A wait.
    await enqueue(schema.admin, schema.table, { task: "fail", key: "k", runAt: far() })
    const producer = await schema.connect()
    await query(producer, "begin")
    await enqueue(producer, schema.table, { task: "fail", key: "k", runAt: far() })
    failGate.resolve()
    await waitForLockedStatement(schema.admin, toStatementLabel("settle:lock-competitor"))
    const d = await enqueue(schema.admin, schema.table, { task: "wait" })
    const beats = log.all("heartbeat").length
    await sleep(2.5 * lease * 1000)
    // The heartbeat went on during the settlement of A, whose row the fail transaction holds.
    expect(log.all("heartbeat").length).toBeGreaterThan(beats + 10)
    expect(log.all("abort")).toEqual([])
    expect(new Set(log.all("start").map(e => e.claim.id))).toEqual(new Set([a.id, c.id]))
    expect(only(await jobs(schema, `id = ${c.id}`)).lease_until!.getTime()).toBeGreaterThan(
      Date.now(),
    )
    await query(producer, "commit")
    expect((await log.waitFor("settled", e => e.claim.id === a.id)).outcome).toBe("superseded")
    const startD = await log.waitFor("start", e => e.claim.id === d.id)
    gates.open(log.all("start", e => e.claim.id === c.id)[0]!.claim.claimId)
    gates.open(startD.claim.claimId)
    for (const id of [c.id, d.id])
      expect((await log.waitFor("settled", e => e.claim.id === id)).outcome).toBe("completed")
    expect(only(await jobs(schema)).state).toBe("pending")
  }, 20_000)

  test("a zombie of a reclaimed job keeps its slot, and the worker does not settle its lost claim", async () => {
    const job = await enqueue(schema.admin, schema.table, { task: "t" })
    const gates = createGates()
    const log = recordEvents()
    startTestWorker(schema, { concurrency: 2, tasks: { t: gates.task }, onEvent: log.onEvent })
    const first = await log.waitFor("start", e => e.claim.id === job.id)
    await reapNow(schema, job.id)
    await log.waitFor("abort", e => e.claim.claimId === first.claim.claimId && e.reason === "lost")
    const second = await log.waitFor("start", e => e.claim.job.attempts === 2)
    const other = await enqueue(schema.admin, schema.table, { task: "t" })
    await sleep(300)
    // The zombie and the second execution of the job take both slots.
    expect(log.all("start", e => e.claim.id === other.id)).toEqual([])
    gates.open(first.claim.claimId)
    expect((await log.waitFor("settled", e => e.claim === first.claim)).outcome).toBe("lost")
    const startOther = await log.waitFor("start", e => e.claim.id === other.id)
    gates.open(second.claim.claimId)
    expect((await log.waitFor("settled", e => e.claim === second.claim)).outcome).toBe("completed")
    gates.open(startOther.claim.claimId)
    await log.waitFor("settled", e => e.claim.id === other.id)
    expect(await jobs(schema)).toEqual([])
  }, 20_000)
})

describe("heartbeat", () => {
  test("a claim takes its lease from the send time of its own statement, not from the start of its cycle", async () => {
    await enqueue(schema.admin, schema.table, { task: "plain" })
    const queued = await enqueue(schema.admin, schema.table, { task: "queued", queue: "q" })
    const release = await holdHook(schema.admin, "claim_plain")
    const gates = createGates({ honorAbort: true })
    const log = recordEvents()
    startTestWorker(schema, {
      driver: schema.createDriver({ hooks: ["claim_plain"] }),
      heartbeatSource: schema.createDriver({ max: 1 }),
      concurrency: 2,
      tasks: { plain: gates.task, queued: gates.task },
      onEvent: log.onEvent,
    })
    await waitForLockedStatement(schema.admin, "kern_test_hook('claim_plain')")
    // The plain phase of the cycle takes longer than the lease.
    await sleep(lease * 1000)
    await release()
    const start = await log.waitFor("start", e => e.claim.id === queued.id)
    await sleep(3 * interval * 1000)
    expect(log.all("abort", e => e.claim.claimId === start.claim.claimId)).toEqual([])
  })

  test("a claim cycle sends no further claim statement once the heartbeat fails during it", async () => {
    await enqueue(schema.admin, schema.table, { task: "plain" })
    const queued = await enqueue(schema.admin, schema.table, { task: "queued", queue: "q" })
    const { proxy, source } = await proxiedHeartbeat()
    const release = await holdHook(schema.admin, "claim_plain")
    const gates = createGates({ honorAbort: true })
    const log = recordEvents()
    startTestWorker(schema, {
      driver: schema.createDriver({ hooks: ["claim_plain"] }),
      heartbeatSource: source,
      concurrency: 2,
      tasks: { plain: gates.task, queued: gates.task },
      onEvent: log.onEvent,
    })
    await waitForLockedStatement(schema.admin, "kern_test_hook('claim_plain')")
    proxy.refuse = true
    proxy.cut()
    await log.waitFor("heartbeat-error")
    await release()
    await log.waitFor("start", e => e.claim.job.task === "plain")
    await sleep(200)
    const claimed = log.all("start", e => e.claim.id === queued.id).length
    proxy.refuse = false
    await log.waitFor("start", e => e.claim.id === queued.id)
    expect(claimed).toBe(0)
  })

  test("heartbeats run one at a time", async () => {
    await enqueue(schema.admin, schema.table, { task: "t" })
    const release = await holdHook(schema.admin, "heartbeat")
    const gates = createGates()
    const log = recordEvents()
    startTestWorker(schema, {
      heartbeatSource: schema.createDriver({ max: 1, hooks: ["heartbeat"] }),
      tasks: { t: gates.task },
      onEvent: log.onEvent,
    })
    await log.waitFor("heartbeat")
    await sleep(5 * interval * 1000)
    const sent = log.all("heartbeat").length
    await release()
    await sleep(3 * interval * 1000)
    expect(sent).toBe(1)
    expect(log.all("heartbeat").length).toBeGreaterThan(1)
  })

  test("a lease whose third exceeds the longest delay of a timer is kept by heartbeats at that delay, not at once", async () => {
    await enqueue(schema.admin, schema.table, { task: "t" })
    const gates = createGates()
    const log = recordEvents()
    startTestWorker(schema, {
      heartbeatSource: schema.createDriver({ max: 1 }),
      tasks: { t: gates.task },
      // A third of this lease is about 386 days, and the longest delay of a timer is under 25.
      lease: 1e8,
      heartbeatInterval: undefined,
      abortMargin: undefined,
      onEvent: log.onEvent,
    })
    await log.waitFor("start")
    await sleep(200)
    // A timer of a longer delay fires at once, which would send heartbeats without a pause.
    expect(log.all("heartbeat").length).toBeLessThanOrEqual(1)
  })

  test("a worker whose heartbeat is stuck does not claim", async () => {
    const first = await enqueue(schema.admin, schema.table, { task: "t" })
    const release = await holdHook(schema.admin, "heartbeat")
    const gates = createGates()
    const log = recordEvents()
    startTestWorker(schema, {
      concurrency: 2,
      heartbeatSource: schema.createDriver({ max: 1, hooks: ["heartbeat"] }),
      tasks: { t: gates.task },
      onEvent: log.onEvent,
    })
    await log.waitFor("start", e => e.claim.id === first.id)
    await log.waitFor("heartbeat")
    await sleep(2 * interval * 1000)
    const late = await enqueue(schema.admin, schema.table, { task: "t" })
    await sleep(200)
    const claimed = log.all("start", e => e.claim.id === late.id).length
    await release()
    await log.waitFor("start", e => e.claim.id === late.id)
    expect(claimed).toBe(0)
  })

  test("a lost claim aborts its handler, and the worker does not settle it", async () => {
    const job = await enqueue(schema.admin, schema.table, { task: "t" })
    const gates = createGates({ honorAbort: true })
    const log = recordEvents()
    startTestWorker(schema, { tasks: { t: gates.task }, onEvent: log.onEvent })
    const first = await log.waitFor("start")
    await reapNow(schema, job.id)
    const end = await log.waitFor("end", e => e.claim === first.claim)
    expect([end.aborted, end.outcome, (end.error as HandlerAbort).reason]).toEqual([
      "lost",
      "retry",
      "lost",
    ])
    expect((await log.waitFor("settled", e => e.claim === first.claim)).outcome).toBe("lost")
    // The worker claims the returned job again; no fail recorded the abort.
    const second = await log.waitFor("start", e => e.claim.job.attempts === 2)
    expect(second.claim.job.last_error).toBe("lease expired")
  })

  test("a claim that returns after the deadline of its lease does not start its handler, and its job retries with the attempt spent", async () => {
    const job = await enqueue(schema.admin, schema.table, { task: "t" })
    const release = await holdHook(schema.admin, "claim_plain")
    const log = recordEvents()
    let calls = 0
    startTestWorker(schema, {
      driver: schema.createDriver({ hooks: ["claim_plain"] }),
      heartbeatSource: schema.createDriver({ max: 1 }),
      tasks: { t: { handler: () => void calls++, backoff: () => 3600 } },
      onEvent: log.onEvent,
    })
    await waitForLockedStatement(schema.admin, "kern_test_hook('claim_plain')")
    // The deadline of the abort comes the lease less the abort margin after the send time of the claim statement.
    await sleep((lease - testTimings.abortMargin) * 1000 + 100)
    await release()
    expect((await log.waitFor("settled")).outcome).toBe("retry")
    expect([calls, log.all("start"), log.all("late-claim").map(e => e.claim.id)]).toEqual([
      0,
      [],
      [job.id],
    ])
    const row = only(await jobs(schema))
    expect([row.state, row.attempts, row.last_error]).toEqual([
      "pending",
      1,
      "lease confirmation deadline passed before the handler started",
    ])
  })

  test("a claim that returns after the deadline of its lease on the last attempt fails the job with the reason late-claim of its failure job", async () => {
    await enqueue(schema.admin, schema.table, { task: "t", maxAttempts: 1 })
    const release = await holdHook(schema.admin, "claim_plain")
    const log = recordEvents()
    startTestWorker(schema, {
      driver: schema.createDriver({ hooks: ["claim_plain"] }),
      heartbeatSource: schema.createDriver({ max: 1 }),
      tasks: { t: { handler: () => {}, failureMaxAttempts: 2 } },
      onEvent: log.onEvent,
    })
    await waitForLockedStatement(schema.admin, "kern_test_hook('claim_plain')")
    await sleep((lease - testTimings.abortMargin) * 1000 + 100)
    await release()
    expect((await log.waitFor("settled")).outcome).toBe("failed")
    const failed = only(await jobs(schema, "task = 't:failure'"))
    expect(failed.payload).toMatchObject({
      reason: "late-claim",
      error: { message: "lease confirmation deadline passed before the handler started" },
    })
  })

  test("a broken heartbeat connection aborts handlers at their deadline and stops claiming until it reconnects", async () => {
    const job = await enqueue(schema.admin, schema.table, { task: "t" })
    const { proxy, source } = await proxiedHeartbeat()
    const gates = createGates({ honorAbort: true })
    const log = recordEvents()
    startTestWorker(schema, {
      concurrency: 2,
      heartbeatSource: source,
      tasks: { t: gates.task, other: gates.task },
      onEvent: log.onEvent,
    })
    const start = await log.waitFor("start")
    proxy.refuse = true
    proxy.cut()
    const cutAt = Date.now()
    await log.waitFor("abort", e => e.reason === "deadline")
    const abortedAfter = Date.now() - cutAt
    // The pool works, so the aborted job is settled as a failure with its backoff.
    expect((await log.waitFor("settled", e => e.claim === start.claim)).outcome).toBe("retry")
    const other = await enqueue(schema.admin, schema.table, { task: "other" })
    await sleep(300)
    const claimed = log.all("start", e => e.claim.id === other.id).length
    proxy.refuse = false
    await log.waitFor("heartbeat-recovered")
    await log.waitFor("start", e => e.claim.id === other.id)
    expect(claimed).toBe(0)
    expect(abortedAfter).toBeLessThan(lease * 1000)
    const row = only(await jobs(schema, `id = ${job.id}`))
    expect([row.state, row.last_error]).toEqual(["pending", "handler aborted: deadline"])
  })

  test("a heartbeat connection whose backend died is replaced, and the handler keeps its lease", async () => {
    await enqueue(schema.admin, schema.table, { task: "t" })
    const gates = createGates({ honorAbort: true })
    const log = recordEvents()
    startTestWorker(schema, {
      heartbeatSource: schema.createDriver({ max: 1, applicationName: "heartbeat" }),
      tasks: { t: gates.task },
      onEvent: log.onEvent,
    })
    const start = await log.waitFor("start")
    await log.waitFor("heartbeat")
    expect(await terminateBackends(schema.admin, "heartbeat")).toBe(1)
    await log.waitFor("heartbeat-error")
    await log.waitFor("heartbeat-recovered")
    await sleep(lease * 1000)
    gates.open(start.claim.claimId)
    expect((await log.waitFor("settled")).outcome).toBe("completed")
    expect(log.all("abort")).toEqual([])
  })
})

describe("worker id", () => {
  test("a worker returns the jobs that an ended worker of its id claimed without waiting for their leases, and runs them with the attempt spent", async () => {
    const job = await enqueue(schema.admin, schema.table, { task: "t" })
    await claimPlain(schema.admin, schema.table, {
      worker: "same",
      tasks: ["t"],
      limit: 1,
      defaultLease: 3600,
    })
    const log = recordEvents()
    startTestWorker(schema, {
      id: "same",
      tasks: { t: { handler: () => {} } },
      onEvent: log.onEvent,
    })
    expect((await log.waitFor("recover")).jobs).toEqual([
      { id: job.id, state: "pending", detached: false },
    ])
    const start = await log.waitFor("start")
    expect([start.claim.job.attempts, start.claim.job.last_error]).toEqual([2, "worker restarted"])
    expect((await log.waitFor("settled")).outcome).toBe("completed")
  })

  test("a worker whose id a running worker holds takes no jobs and leaves the jobs of that worker until it stops", async () => {
    const first = await enqueue(schema.admin, schema.table, { task: "t" })
    const gates = createGates()
    const runningLog = recordEvents()
    const running = startTestWorker(schema, {
      id: "same",
      heartbeatSource: schema.createDriver({ max: 1 }),
      tasks: { t: gates.task },
      onEvent: runningLog.onEvent,
    })
    const started = await runningLog.waitFor("start")
    const second = await enqueue(schema.admin, schema.table, { task: "t" })
    const log = recordEvents()
    const starting = startTestWorker(schema, {
      id: "same",
      // A pool of its own, so that a lock that the running worker left on its connection would keep this one from taking it.
      heartbeatSource: schema.createDriver({ max: 1 }),
      tasks: { t: { handler: () => {} } },
      onEvent: log.onEvent,
    })
    const busy = await log.waitFor("heartbeat-error")
    await log.waitFor("heartbeat-error", e => e !== busy)
    expect((busy.error as Error).message).toBe(
      `Worker id same is in use in schema ${schema.name}; this worker takes no jobs until the id is free`,
    )
    expect(starting.state).toBe("starting")
    expect((await jobs(schema)).map(r => [r.id, r.state])).toEqual([
      [first.id, "active"],
      [second.id, "pending"],
    ])
    const stopped = running.stop()
    gates.open(started.claim.claimId)
    await stopped
    await starting.ready
    expect((await log.waitFor("settled")).claim.job.id).toBe(second.id)
    expect(log.all("recover")).toEqual([])
  })

  test("a worker stopped while it returns the jobs of its id holds the lock until its statement ends, claims no jobs, and reports no claim error", async () => {
    const job = await enqueue(schema.admin, schema.table, { task: "t" })
    await claimPlain(schema.admin, schema.table, {
      worker: "same",
      tasks: ["t"],
      limit: 1,
      defaultLease: 3600,
    })
    const release = await holdHook(schema.admin, "reap")
    openAll.push(() => void release())
    let statements = 0
    const stoppedLog = recordEvents()
    const stopped = startTestWorker(schema, {
      id: "same",
      onEvent: stoppedLog.onEvent,
      driver: {
        ...schema.driver,
        execute(statement) {
          statements++
          return schema.driver.execute(statement)
        },
      },
      heartbeatSource: schema.createDriver({ max: 1, hooks: ["reap"] }),
      tasks: { t: { handler: () => {} } },
    })
    await waitForLockedStatement(schema.admin, "kern_test_hook('reap')")
    await expect(stopped.stop({ force: true })).rejects.toThrow("The worker stop was forced")
    const log = recordEvents()
    const next = startTestWorker(schema, {
      id: "same",
      heartbeatSource: schema.createDriver({ max: 1 }),
      tasks: { t: { handler: () => {} } },
      onEvent: log.onEvent,
    })
    await log.waitFor("heartbeat-error")
    expect(next.state).toBe("starting")
    await release()
    expect((await log.waitFor("settled")).claim.job.id).toBe(job.id)
    expect(statements).toBe(0)
    expect(stoppedLog.all("recover").length).toBe(1)
    expect(stoppedLog.all("claim-error")).toEqual([])
  })

  test.each([false, true])(
    "a worker stopped while a batch of the return waits for a producer sends no further statement of the return, with force %p",
    async force => {
      await enqueue(schema.admin, schema.table, { task: "t", key: "k" })
      await claimPlain(schema.admin, schema.table, {
        worker: "same",
        tasks: ["t"],
        limit: 1,
        defaultLease: 3600,
      })
      const producer = await schema.connect()
      await query(producer, "begin")
      await enqueue(producer, schema.table, { task: "t", key: "k" })
      const heartbeat = createWatchedHeartbeat(schema.createDriver({ max: 1 }))
      const worker = startTestWorker(schema, {
        id: "same",
        heartbeatSource: heartbeat.driver,
        tasks: { t: { handler: () => {} } },
      })
      await waitForLockedStatement(schema.admin, toStatementLabel("reap:batch"))
      const sent = heartbeat.texts.length
      const stopped = worker.stop({ force })
      if (force) await expect(stopped).rejects.toThrow("The worker stop was forced")
      else await stopped
      // The batch gives up its wait for the producer after a second, and would then return its rows one by one before the session closes.
      await heartbeat.released
      await query(producer, "rollback")
      expect(heartbeat.texts.slice(sent).filter(text => text.includes(schema.table.name))).toEqual(
        [],
      )
    },
  )

  test("a worker whose heartbeat fails during the return claims nothing until a heartbeat succeeds", async () => {
    const job = await enqueue(schema.admin, schema.table, { task: "t" })
    await claimPlain(schema.admin, schema.table, {
      worker: "same",
      tasks: ["t"],
      limit: 1,
      defaultLease: 3600,
    })
    const release = await holdHook(schema.admin, "reap")
    openAll.push(() => void release())
    const heartbeat = createWatchedHeartbeat(schema.createDriver({ max: 1, hooks: ["reap"] }))
    let statements = 0
    const log = recordEvents()
    startTestWorker(schema, {
      id: "same",
      driver: {
        ...schema.driver,
        execute(statement) {
          statements++
          return schema.driver.execute(statement)
        },
      },
      heartbeatSource: heartbeat.driver,
      tasks: { t: { handler: () => {} } },
      onEvent: log.onEvent,
    })
    await waitForLockedStatement(schema.admin, "kern_test_hook('reap')")
    heartbeat.failing = true
    await log.waitFor("heartbeat-error", e => e.error instanceof QueryError)
    await release()
    await log.waitFor("recover")
    // The second batch returns none and ends the return, after which the claim cycle would claim at once.
    // With the periodic reaper disabled, every reap batch observed here belongs to the return of the jobs of the worker.
    const isBatch = (text: string) => getStatementLabel(text) === toStatementLabel("reap:batch")
    while (heartbeat.answered.filter(isBatch).length < 2) await sleep(10)
    await timers.setImmediate()
    expect(statements).toBe(0)
    heartbeat.failing = false
    expect((await log.waitFor("settled")).claim.job.id).toBe(job.id)
  })

  test(
    "a job of the id whose key a producer holds does not stop the return of the later jobs of the id",
    async () => {
      const ids: string[] = []
      for (let i = 0; i <= 100; i++)
        ids.push((await enqueue(schema.admin, schema.table, { task: "t", key: `k${i}` })).id)
      await claimPlain(schema.admin, schema.table, {
        worker: "same",
        tasks: ["t"],
        limit: ids.length,
        defaultLease: 3600,
      })
      const producer = await schema.connect()
      await query(producer, "begin")
      await enqueue(producer, schema.table, { task: "t", key: "k0" })
      const log = recordEvents()
      startTestWorker(schema, { id: "same", tasks: {}, onEvent: log.onEvent })
      await log.waitFor("recover", e => e.jobs.some(j => j.id === ids.at(-1)), 15_000)
      const active = (await jobs(schema)).filter(r => r.state === "active").map(r => r.id)
      await query(producer, "rollback")
      expect(active).toEqual(ids.slice(0, 1))
    },
    { timeout: 20_000 },
  )
})

describe("failures", () => {
  test("a failed handler retries after the backoff of its task or the delay of a RetryError; a TerminalError fails the job for good", async () => {
    const retried = await enqueue(schema.admin, schema.table, { task: "retry" })
    const delayed = await enqueue(schema.admin, schema.table, { task: "retryError" })
    const lastAttempt = await enqueue(schema.admin, schema.table, {
      task: "retryError",
      maxAttempts: 1,
    })
    const terminal = await enqueue(schema.admin, schema.table, { task: "terminal" })
    const log = recordEvents()
    startTestWorker(schema, {
      concurrency: 4,
      tasks: {
        retry: {
          handler: () => {
            throw new Error("boom")
          },
          backoff: attempt => 100 * attempt,
        },
        retryError: {
          handler: () => {
            throw new RetryError("rate limited", { delay: 200 })
          },
          backoff: () => 1,
        },
        terminal: {
          handler: () => {
            throw new TerminalError("bad payload")
          },
        },
      },
      onEvent: log.onEvent,
    })
    const outcomes = await Promise.all(
      [retried, delayed, lastAttempt, terminal].map(j =>
        log.waitFor("settled", e => e.claim.id === j.id),
      ),
    )
    // The attempt of a RetryError counts, so the last one fails for good.
    expect(outcomes.map(e => e.outcome)).toEqual(["retry", "retry", "failed", "failed"])
    const [r, d, l, t] = await jobs(schema)
    expect([r, d, l, t].map(row => [row!.state, row!.last_error])).toEqual([
      ["pending", "boom"],
      ["pending", "rate limited"],
      ["failed", "rate limited"],
      ["failed", "bad payload"],
    ])
    expect(r!.not_before!.getTime() - Date.now()).toBeGreaterThan(90_000)
    expect(d!.not_before!.getTime() - Date.now()).toBeGreaterThan(190_000)
    expect(d!.not_before!.getTime() - Date.now()).toBeLessThanOrEqual(200_000)
    expect(log.all("backoff-error")).toEqual([])
  })

  test("a message with U+0000, which PostgreSQL rejects in text, is escaped, and the error decides the outcome", async () => {
    const terminal = await enqueue(schema.admin, schema.table, { task: "terminal" })
    const delayed = await enqueue(schema.admin, schema.table, { task: "retryError" })
    const log = recordEvents()
    startTestWorker(schema, {
      concurrency: 2,
      tasks: {
        terminal: {
          handler: () => {
            throw new TerminalError("bad\0payload")
          },
        },
        retryError: {
          handler: () => {
            throw new RetryError("rate\0limited", { delay: 200 })
          },
        },
      },
      onEvent: log.onEvent,
    })
    const outcomes = await Promise.all(
      [terminal, delayed].map(j => log.waitFor("settled", e => e.claim.id === j.id)),
    )
    expect(outcomes.map(e => e.outcome)).toEqual(["failed", "retry"])
    const [t, d] = await jobs(schema)
    expect([t!.last_error, d!.last_error]).toEqual(["bad\\u0000payload", "rate\\u0000limited"])
    expect(d!.not_before!.getTime() - Date.now()).toBeGreaterThan(190_000)
  })

  test("a backoff that throws or gives a delay outside its range is reported, and the job retries with the default backoff", async () => {
    const tasks = ["throws", "nan", "infinite", "huge", "negative"]
    const ids = await Promise.all(tasks.map(task => enqueue(schema.admin, schema.table, { task })))
    const log = recordEvents()
    const boom = () => {
      throw new Error("boom")
    }
    startTestWorker(schema, {
      concurrency: tasks.length,
      tasks: {
        throws: {
          handler: boom,
          backoff: () => {
            throw new Error("policy")
          },
        },
        nan: { handler: boom, backoff: () => NaN },
        infinite: { handler: boom, backoff: () => Infinity },
        // Finite, but beyond the intervals of PostgreSQL.
        huge: { handler: boom, backoff: () => 1e20 },
        negative: { handler: boom, backoff: () => -5 },
      },
      onEvent: log.onEvent,
    })
    for (const { id } of ids) {
      expect((await log.waitFor("settled", e => e.claim.id === id)).outcome).toBe("retry")
      expect(log.all("backoff-error", e => e.claim.id === id)).toHaveLength(1)
      const row = only(await jobs(schema, `id = ${id}`))
      // The default backoff of the first attempt is 2 seconds.
      expect(row.not_before!.getTime() - Date.now()).toBeGreaterThan(1000)
    }
  })

  test("a handler that throws undefined fails its job, for good on its last attempt", async () => {
    const job = await enqueue(schema.admin, schema.table, { task: "t", maxAttempts: 1 })
    const log = recordEvents()
    startTestWorker(schema, {
      tasks: {
        t: {
          handler() {
            throw undefined
          },
        },
      },
      onEvent: log.onEvent,
    })
    expect((await log.waitFor("settled")).outcome).toBe("failed")
    expect(log.all("end").map(e => e.outcome)).toEqual(["terminal"])
    const row = only(await jobs(schema, `id = ${job.id}`))
    expect([row.state, row.last_error]).toEqual(["failed", "undefined"])
  })

  test("a handler that throws a value whose inspection or string conversion throws fails its job", async () => {
    const plain = await enqueue(schema.admin, schema.table, { task: "nullPrototype" })
    const hostile = await enqueue(schema.admin, schema.table, { task: "throwingGetter" })
    const proxy = await enqueue(schema.admin, schema.table, { task: "throwingPrototype" })
    const log = recordEvents()
    const thrower = (value: () => unknown): WorkerTask => ({
      handler() {
        throw value()
      },
    })
    startTestWorker(schema, {
      concurrency: 3,
      tasks: {
        nullPrototype: thrower(() => Object.create(null)),
        throwingGetter: thrower(() =>
          Object.create(null, {
            [Symbol.toStringTag]: {
              get() {
                throw new Error("getter")
              },
            },
          }),
        ),
        // `instanceof` reads the prototype of the value, which the trap of the proxy refuses.
        throwingPrototype: thrower(
          () =>
            new Proxy(
              {},
              {
                getPrototypeOf() {
                  throw new Error("trap")
                },
              },
            ),
        ),
      },
      onEvent: log.onEvent,
    })
    for (const job of [plain, hostile, proxy]) {
      expect((await log.waitFor("settled", e => e.claim.id === job.id)).outcome).toBe("retry")
      expect(only(await jobs(schema, `id = ${job.id}`)).last_error).toBe("unprintable error")
    }
    // Telling the error classes of the proxy fails, so its job retries with the default backoff.
    expect(log.all("backoff-error").map(e => e.claim.id)).toEqual([proxy.id])
  })
})

describe("stop", () => {
  test("a handler that ends before abortAfter completes its job, and nothing is aborted", async () => {
    await enqueue(schema.admin, schema.table, { task: "t" })
    const gates = createGates({ honorAbort: true })
    const log = recordEvents()
    const worker = startTestWorker(schema, { tasks: { t: gates.task }, onEvent: log.onEvent })
    const start = await log.waitFor("start")
    const stopped = worker.stop({ abortAfter: 60 })
    await sleep(3 * interval * 1000)
    gates.open(start.claim.claimId)
    await stopped
    expect([log.all("abort"), log.all("settled").map(e => e.outcome)]).toEqual([[], ["completed"]])
    expect(await jobs(schema)).toEqual([])
  })

  test("after abortAfter, a handler that throws, also an error that wraps the abort, returns its job at once with the attempt given back, also on its last attempt", async () => {
    const job = await enqueue(schema.admin, schema.table, { task: "t", maxAttempts: 1 })
    const log = recordEvents()
    const heartbeatSource = schema.createDriver({ max: 1 })
    let thrown: unknown
    const worker = startTestWorker(schema, {
      heartbeatSource,
      tasks: {
        t: {
          // Rejects with an AbortError whose cause is the reason of the signal.
          handler: (_job, { signal }) =>
            timers.setTimeout(3600_000, undefined, { signal }).catch((e: unknown) => {
              thrown = e
              throw e
            }),
        },
      },
      onEvent: log.onEvent,
    })
    await log.waitFor("start")
    const stopAt = Date.now()
    await worker.stop({ abortAfter: 0.2 })
    expect(log.events.map(e => e.type).filter(t => !t.startsWith("heartbeat"))).toEqual([
      "start",
      "abort",
      "end",
      "settled",
    ])
    expect(log.all("abort")[0]!.reason).toBe("stop")
    expect([(thrown as Error).name, (thrown as Error).cause instanceof HandlerAbort]).toEqual([
      "AbortError",
      true,
    ])
    expect(log.all("end")[0]!.outcome).toBe("requeue")
    expect(log.all("settled")[0]!.outcome).toBe("requeued")
    const row = only(await jobs(schema, `id = ${job.id}`))
    expect([row.state, row.attempts, row.last_error, row.claim_id]).toEqual([
      "pending",
      0,
      null,
      null,
    ])
    expect(Date.now() - stopAt).toBeGreaterThanOrEqual(200)
    // The pool of one connection gives it once the heartbeat has released it.
    expect(await query(heartbeatSource, "select 1 as one")).toEqual([{ one: "1" }])
    await heartbeatSource.close()
  })

  test("after the abort, a TerminalError fails the job, a RetryError retries it with the attempt spent, and a handler that returns completes it", async () => {
    const ids = {
      terminal: (await enqueue(schema.admin, schema.table, { task: "terminal" })).id,
      retry: (await enqueue(schema.admin, schema.table, { task: "retry" })).id,
      returns: (await enqueue(schema.admin, schema.table, { task: "returns" })).id,
    }
    const log = recordEvents()
    const afterAbort =
      (end: () => void): WorkerTask["handler"] =>
      (_job, { signal }) =>
        new Promise<void>(resolve => signal.addEventListener("abort", () => resolve())).then(end)
    const worker = startTestWorker(schema, {
      concurrency: 3,
      tasks: {
        terminal: {
          handler: afterAbort(() => {
            throw new TerminalError("gone")
          }),
        },
        retry: {
          handler: afterAbort(() => {
            throw new RetryError("later", { delay: 3600 })
          }),
        },
        returns: { handler: afterAbort(() => {}) },
      },
      onEvent: log.onEvent,
    })
    for (const task of ["terminal", "retry", "returns"])
      await log.waitFor("start", e => e.claim.job.task === task)
    await worker.stop({ abortAfter: 0 })
    const outcomes = Object.fromEntries(log.all("settled").map(e => [e.claim.job.task, e.outcome]))
    expect(outcomes).toEqual({ terminal: "failed", retry: "retry", returns: "completed" })
    const decided = Object.fromEntries(log.all("end").map(e => [e.claim.job.task, e.outcome]))
    expect(decided).toEqual({ terminal: "terminal", retry: "retry", returns: "complete" })
    const rows = await jobs(schema)
    expect(rows.map(r => [r.id, r.state, r.attempts, r.last_error])).toEqual([
      [ids.terminal, "failed", 1, "gone"],
      [ids.retry, "pending", 1, "later"],
    ])
  })

  test("after the abort, a RetryError whose delay cannot be read spends the attempt, and the failed read is reported", async () => {
    const job = await enqueue(schema.admin, schema.table, { task: "t", maxAttempts: 1 })
    const log = recordEvents()
    const trap = new Error("trap")
    const worker = startTestWorker(schema, {
      tasks: {
        t: {
          handler: async (_job, { signal }) => {
            await new Promise<void>(resolve => signal.addEventListener("abort", () => resolve()))
            throw new Proxy(new RetryError("later", { delay: 3600 }), {
              get(target, property, receiver) {
                if (property === "delay") throw trap
                return Reflect.get(target, property, receiver) as unknown
              },
            })
          },
        },
      },
      onEvent: log.onEvent,
    })
    await log.waitFor("start")
    await worker.stop({ abortAfter: 0 })
    expect(log.all("end").map(e => [e.aborted, e.outcome])).toEqual([["stop", "terminal"]])
    expect(log.all("backoff-error").map(e => e.error)).toEqual([trap])
    expect(log.all("settled").map(e => e.outcome)).toEqual(["failed"])
    const row = only(await jobs(schema, `id = ${job.id}`))
    expect([row.state, row.attempts, row.last_error]).toEqual(["failed", 1, "later"])
  })

  test("a job interrupted by stop merges with the pending job of its key", async () => {
    const job = await enqueue(schema.admin, schema.table, { task: "t", key: "k" })
    const gates = createGates({ honorAbort: true })
    const log = recordEvents()
    const worker = startTestWorker(schema, { tasks: { t: gates.task }, onEvent: log.onEvent })
    await log.waitFor("start")
    const next = await enqueue(schema.admin, schema.table, { task: "t", key: "k", runAt: far() })
    await worker.stop({ abortAfter: 0 })
    expect(log.all("settled").map(e => [e.claim.id, e.outcome])).toEqual([[job.id, "superseded"]])
    expect((await jobs(schema)).map(r => [r.id, r.state, r.attempts])).toEqual([
      [next.id, "pending", 0],
    ])
  })

  test("a claim that returns after stop began does not start its handler, and its job returns with the attempt given back", async () => {
    const job = await enqueue(schema.admin, schema.table, { task: "t", maxAttempts: 1 })
    const release = await holdHook(schema.admin, "claim_plain")
    const log = recordEvents()
    let calls = 0
    const worker = startTestWorker(schema, {
      driver: schema.createDriver({ hooks: ["claim_plain"] }),
      heartbeatSource: schema.createDriver({ max: 1 }),
      tasks: { t: { handler: () => void calls++ } },
      onEvent: log.onEvent,
    })
    await waitForLockedStatement(schema.admin, "kern_test_hook('claim_plain')")
    const stopped = worker.stop()
    await release()
    await stopped
    expect([calls, log.all("start"), log.all("settled").map(e => e.outcome)]).toEqual([
      0,
      [],
      ["requeued"],
    ])
    const row = only(await jobs(schema, `id = ${job.id}`))
    expect([row.state, row.attempts]).toEqual(["pending", 0])
  })

  test("a claim that returns after stop began and after the deadline of its lease returns its job with the attempt given back", async () => {
    await enqueue(schema.admin, schema.table, { task: "t" })
    const release = await holdHook(schema.admin, "claim_plain")
    const log = recordEvents()
    const worker = startTestWorker(schema, {
      driver: schema.createDriver({ hooks: ["claim_plain"] }),
      heartbeatSource: schema.createDriver({ max: 1 }),
      tasks: { t: { handler: () => {} } },
      onEvent: log.onEvent,
    })
    await waitForLockedStatement(schema.admin, "kern_test_hook('claim_plain')")
    // The deadline of the abort comes the lease less the abort margin after the send time of the claim statement.
    await sleep((lease - testTimings.abortMargin) * 1000 + 100)
    const stopped = worker.stop()
    await release()
    await stopped
    expect([log.all("late-claim"), log.all("settled").map(e => e.outcome)]).toEqual([
      [],
      ["requeued"],
    ])
    expect(only(await jobs(schema)).attempts).toBe(0)
  })

  test("at its timeout, stop rejects and leaves a handler that ignores the abort: nothing extends its lease or settles its claim, also after its job runs again", async () => {
    const job = await enqueue(schema.admin, schema.table, { task: "t" })
    const gates = createGates()
    const log = recordEvents()
    const worker = startTestWorker(schema, { tasks: { t: gates.task }, onEvent: log.onEvent })
    const orphan = await log.waitFor("start")
    const stopAt = Date.now()
    const error = await worker.stop({ abortAfter: 0, timeout: 0.3 }).catch((e: unknown) => e)
    expect((error as Error).message).toBe(
      "The worker did not stop within 0.3 s; jobs still running or settling: 1",
    )
    expect(Date.now() - stopAt).toBeLessThan(lease * 1000)
    expect(log.all("abort").map(e => e.reason)).toEqual(["stop"])
    // A heartbeat in flight at the timeout may still extend the lease once.
    await sleep(3 * interval * 1000)
    const beats = log.all("heartbeat").length
    const leaseUntil = only(await jobs(schema)).lease_until
    await sleep(3 * interval * 1000)
    expect(log.all("heartbeat").length).toBe(beats)
    expect(only(await jobs(schema)).lease_until).toEqual(leaseUntil)
    // Another worker runs the job again, and the orphan then ends.
    await reapNow(schema, job.id)
    const other = recordEvents()
    const gatesOther = createGates()
    startTestWorker(schema, { tasks: { t: gatesOther.task }, onEvent: other.onEvent })
    const again = await other.waitFor("start")
    gates.open(orphan.claim.claimId)
    expect((await log.waitFor("settled")).outcome).toBe("stopped")
    const row = only(await jobs(schema))
    expect([row.state, row.claim_id, row.attempts]).toEqual(["active", again.claim.claimId, 2])
    gatesOther.open(again.claim.claimId)
    expect((await other.waitFor("settled")).outcome).toBe("completed")
  })

  test("a timeout that comes before abortAfter aborts the handlers after the worker gave up, so a handler that ends on the abort is not settled", async () => {
    await enqueue(schema.admin, schema.table, { task: "t" })
    const gates = createGates({ honorAbort: true })
    const log = recordEvents()
    const worker = startTestWorker(schema, { tasks: { t: gates.task }, onEvent: log.onEvent })
    await log.waitFor("start")
    const error = await worker.stop({ abortAfter: 60, timeout: 0.2 }).catch((e: unknown) => e)
    expect((error as Error).message).toStartWith("The worker did not stop within 0.2 s")
    expect((await log.waitFor("settled")).outcome).toBe("stopped")
    const row = only(await jobs(schema))
    expect([row.state, row.attempts]).toEqual(["active", 1])
  })

  test("force gives up a stop in progress at once, then aborts the handlers, so a handler that ends right after it is not settled, and its job on its last attempt fails once its lease expires", async () => {
    const job = await enqueue(schema.admin, schema.table, { task: "t", maxAttempts: 1 })
    const gates = createGates({ honorAbort: true })
    const log = recordEvents()
    const worker = startTestWorker(schema, { tasks: { t: gates.task }, onEvent: log.onEvent })
    const start = await log.waitFor("start")
    const stopped = worker.stop({ abortAfter: 60 })
    expect(worker.stop({ force: true })).toBe(stopped)
    gates.open(start.claim.claimId)
    const error = await stopped.catch((e: unknown) => e)
    expect((error as Error).message).toStartWith("The worker stop was forced")
    expect(log.all("abort").map(e => e.reason)).toEqual(["stop"])
    expect((await log.waitFor("settled")).outcome).toBe("stopped")
    const row = only(await jobs(schema))
    expect([row.state, row.attempts]).toEqual(["active", 1])
    await reapNow(schema, job.id)
    expect(only(await jobs(schema)).state).toBe("failed")
  })

  test("a thrown value whose inspection forces the stop does not abort the signal of its handler, which has ended, and its error is reported as a failure", async () => {
    await enqueue(schema.admin, schema.table, { task: "t" })
    const log = recordEvents()
    let handlerSignal: AbortSignal | undefined
    const worker = startTestWorker(schema, {
      tasks: {
        t: {
          handler(_job, { signal }) {
            handlerSignal = signal
            // `instanceof` reads the prototype of the value, and the trap forces the stop.
            throw new Proxy(new Error("boom"), {
              getPrototypeOf(target) {
                worker.stop({ force: true }).catch(() => {})
                return Reflect.getPrototypeOf(target)
              },
            })
          },
        },
      },
      onEvent: log.onEvent,
    })
    const end = await log.waitFor("end")
    expect([end.outcome, end.aborted, handlerSignal!.aborted]).toEqual(["retry", null, false])
    await expect(worker.stop()).rejects.toThrow("The worker stop was forced")
    expect(log.all("abort")).toEqual([])
  })

  test("a stop that has resolved keeps its outcome after a force", async () => {
    const worker = startTestWorker(schema, { tasks: {} })
    const stopped = worker.stop()
    await stopped
    expect(worker.stop({ force: true })).toBe(stopped)
    await stopped
    expect(worker.state).toBe("stopped")
  })

  test("a session that the heartbeat opens right after a force sends no statement, and a failed release of its connection is reported", async () => {
    const acquiring = Promise.withResolvers<void>()
    const arrived = Promise.withResolvers<Connection>()
    const statements: string[] = []
    let releases = 0
    const connection: Connection = {
      async execute({ text }) {
        statements.push(text)
        return { command: "SELECT", count: 0, columns: [], rows: [] }
      },
      async executeScript() {},
      async release() {
        releases++
        throw releaseError
      },
    }
    const releaseError = new Error("release failed")
    const log = recordEvents()
    const worker = startTestWorker(schema, {
      tasks: {},
      onEvent: log.onEvent,
      heartbeatSource: createAcquireOnlyDriver(() => {
        acquiring.resolve()
        return arrived.promise
      }),
    })
    await acquiring.promise
    const stopped = worker.stop({ force: true })
    arrived.resolve(connection)
    await expect(stopped).rejects.toThrow("The worker stop was forced")
    await log.waitFor("release-error")
    expect([statements, releases]).toEqual([[], 1])
    expect(log.all("release-error")).toEqual([{ type: "release-error", error: releaseError }])
  })

  test("a claim that returns after the timeout of stop sends nothing, and its job stays claimed until its lease expires", async () => {
    await enqueue(schema.admin, schema.table, { task: "t" })
    const release = await holdHook(schema.admin, "claim_plain")
    const log = recordEvents()
    let statements = 0
    const pool = schema.createDriver({ hooks: ["claim_plain"] })
    const worker = startTestWorker(schema, {
      driver: {
        ...pool,
        execute(statement) {
          statements++
          return pool.execute(statement)
        },
        acquire() {
          statements++
          return pool.acquire()
        },
      },
      heartbeatSource: schema.createDriver({ max: 1 }),
      tasks: { t: { handler: () => {} } },
      onEvent: log.onEvent,
    })
    await waitForLockedStatement(schema.admin, "kern_test_hook('claim_plain')")
    const sent = statements
    await expect(worker.stop({ timeout: 0.2 })).rejects.toThrow("did not stop within 0.2 s")
    await release()
    while (only(await jobs(schema)).state !== "active") await sleep(10)
    await sleep(200)
    expect([statements - sent, log.all("settled"), log.all("start")]).toEqual([0, [], []])
  })

  test.each(["its connection", "confirmation that its lock timeout was set"])(
    "a check of a schedule that gets %s after the timeout of stop writes nothing",
    async gated => {
      const reached = Promise.withResolvers<void>()
      const gate = Promise.withResolvers<void>()
      const released = Promise.withResolvers<void>()
      openAll.push(gate.resolve)
      const hold = async () => {
        reached.resolve()
        await gate.promise
      }
      let resolvedOptions = 0
      const task = defineTask("report").withOptions(() => {
        resolvedOptions++
        return {}
      })
      const worker = startTestWorker(schema, {
        driver: {
          ...schema.driver,
          async acquire() {
            if (gated === "its connection") await hold()
            const connection = await schema.driver.acquire()
            return {
              async execute(statement) {
                const result = await connection.execute(statement)
                if (gated !== "its connection" && statement.parameters.includes("lock_timeout")) {
                  await hold()
                }
                return result
              },
              executeScript: text => connection.executeScript(text),
              async release() {
                await connection.release()
                released.resolve()
              },
            }
          },
        },
        heartbeatSource: schema.createDriver({ max: 1 }),
        tasks: {},
        schedules: [toWorkerSchedule(defineSchedule(task, { cron: { minute: "*" } }))],
      })
      await reached.promise
      await expect(worker.stop({ timeout: 0 })).rejects.toThrow("did not stop within 0 s")
      gate.resolve()
      await released.promise
      expect(resolvedOptions).toBe(0)
      expect(await jobs(schema)).toEqual([])
      expect(await query(schema.admin, `select id from "${schema.name}".schedule`)).toEqual([])
    },
  )

  test("at its timeout, stop does not wait for a stuck heartbeat, whose connection returns to its pool once the heartbeat ends", async () => {
    await enqueue(schema.admin, schema.table, { task: "t" })
    const gates = createGates({ honorAbort: true })
    const log = recordEvents()
    const heartbeatSource = schema.createDriver({ max: 1, hooks: ["heartbeat"] })
    const worker = startTestWorker(schema, {
      heartbeatSource,
      tasks: { t: gates.task },
      onEvent: log.onEvent,
    })
    await log.waitFor("start")
    await log.waitFor("heartbeat")
    const release = await holdHook(schema.admin, "heartbeat")
    await waitForLockedStatement(schema.admin, "kern_test_hook('heartbeat')")
    await expect(worker.stop({ abortAfter: 0, timeout: 0.5 })).rejects.toThrow(
      "did not stop within 0.5 s",
    )
    expect(log.all("settled").map(e => e.outcome)).toEqual(["requeued"])
    await release()
    // The pool of one connection gives it once the heartbeat has released it.
    expect(await query(heartbeatSource, "select 1 as one")).toEqual([{ one: "1" }])
    await heartbeatSource.close()
  })

  test("stop aborts handlers before it waits for a claim cycle that waits for a connection a handler holds", async () => {
    await enqueue(schema.admin, schema.table, { task: "t" })
    const pool = schema.createDriver({ max: 1 })
    const held = Promise.withResolvers<void>()
    const log = recordEvents()
    const worker = startTestWorker(schema, {
      driver: pool,
      heartbeatSource: schema.createDriver({ max: 1 }),
      concurrency: 2,
      tasks: {
        t: {
          async handler(_job, { signal }) {
            const connection = await pool.acquire()
            held.resolve()
            await new Promise(resolve => signal.addEventListener("abort", resolve))
            await connection.release()
            throw signal.reason
          },
        },
      },
      onEvent: log.onEvent,
    })
    await held.promise
    // The next claim cycle waits for the only connection of the pool.
    await sleep(5 * testTimings.pollInterval * 1000)
    const stopped = await Promise.race([
      worker.stop({ abortAfter: 0 }).then(() => true),
      sleep(3000).then(() => false),
    ])
    expect(stopped).toBe(true)
    expect(log.all("settled").map(e => e.outcome)).toEqual(["requeued"])
  })
})
