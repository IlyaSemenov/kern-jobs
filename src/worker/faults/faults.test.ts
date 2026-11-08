// Process-level fault injection on worker processes of the runtime: SIGKILL at chosen points, connection cuts around COMMIT, heartbeat loss, zombie handlers, slots taken until settlement, stuck heartbeats, a long producer transaction, and the stop on SIGTERM and SIGINT.
import { afterEach, beforeEach, describe, expect, test } from "bun:test"
import { existsSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"

import { waitForLock, waitForLockedStatement } from "kern-db/testing"

import { claimPlain } from "../../core/claim"
import { enqueue } from "../../core/enqueue"
import { reap } from "../../core/lease"
import { query } from "../../core/statement"
import {
  createTestSchema,
  databaseUrl,
  holdHook,
  jobs,
  sleep,
  type TestSchema,
} from "../../core/testing"

import { type Proxy, proxyTo } from "./proxy"
import type { WorkerConfig } from "./worker"

let schema: TestSchema
let dir: string
const procs: Bun.Subprocess[] = []
const proxies: Proxy[] = []

beforeEach(async () => {
  schema = await createTestSchema()
  dir = mkdtempSync(join(tmpdir(), "kern-jobs-faults-"))
})
afterEach(async () => {
  for (const p of procs.splice(0)) p.kill(9)
  await Promise.all(proxies.splice(0).map(p => p.close()))
  await sleep(100)
  await schema.dispose()
})

const DATABASE_URL = databaseUrl()

/** Starts a proxy to the test database and returns it with the URL through it. */
async function proxy() {
  const p = await proxyTo(DATABASE_URL)
  proxies.push(p.proxy)
  return p
}

const completed = (id: string) => (e: Event) =>
  e.ev === "settled" && e.outcome === "completed" && e.id === id

interface Event {
  t: number
  w: string
  ev: string
  [k: string]: any
}

/** Spawns a worker process and returns its log reader. */
function spawnWorker(id: string, overrides: Partial<WorkerConfig> = {}) {
  const cfg: WorkerConfig = {
    id,
    role: "worker",
    schemaName: schema.name,
    mainUrl: DATABASE_URL,
    heartbeatUrl: DATABASE_URL,
    log: join(dir, `${id}.jsonl`),
    tasks: ["t"],
    slots: 2,
    lease: 1,
    heartbeatMs: 200,
    marginMs: 300,
    handlerMs: 300,
    honorAbort: true,
    ...overrides,
  }
  const proc = Bun.spawn(["bun", join(import.meta.dir, "worker.ts")], {
    env: { ...process.env, FAULT_CONFIG: JSON.stringify(cfg) },
    stdout: "inherit",
    stderr: "inherit",
  })
  procs.push(proc)
  const events = (): Event[] =>
    existsSync(cfg.log)
      ? readFileSync(cfg.log, "utf8")
          .trim()
          .split("\n")
          .filter(Boolean)
          .map(l => JSON.parse(l))
      : []
  const waitFor = async (pred: (e: Event) => boolean, timeoutMs = 10_000): Promise<Event> => {
    const deadline = Date.now() + timeoutMs
    while (Date.now() < deadline) {
      const e = events().find(pred)
      if (e) return e
      await sleep(20)
    }
    throw new Error(`${id}: event not seen; log: ${JSON.stringify(events().slice(-5))}`)
  }
  const kill = async () => {
    proc.kill(9)
    await proc.exited
  }
  return { cfg, proc, events, waitFor, kill }
}

const row = async (id: string) => (await jobs(schema, `id = ${id}`))[0]
const backendAlive = async (pid: number) =>
  (await query(schema.admin, `select 1 from pg_stat_activity where pid = $1::text::int`, [pid]))
    .length > 0

/** Reaps until the predicate holds for the row. */
async function reapUntil(id: string, pred: (r: any) => boolean, timeoutMs = 5000) {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    await reap(schema.admin, schema.table)
    const r = await row(id)
    if (r && pred(r)) return r
    await sleep(100)
  }
  throw new Error(`row ${id} did not reach the expected state`)
}

describe("SIGKILL during claiming", () => {
  test("inside the claim statement, default settings: the orphaned backend commits the claim; the reaper recovers it", async () => {
    const job = await enqueue(schema.admin, schema.table, { task: "t" })
    const release = await holdHook(schema.admin, "claim_plain")
    const a = spawnWorker("a", { hooks: ["claim_plain"], poolSize: 1 })
    const ready = await a.waitFor(e => e.ev === "ready")
    await waitForLock(schema.admin, ready.mainPid)
    await a.kill()
    await release()
    await sleep(200)
    expect([(await row(job.id))!.state, (await row(job.id))!.locked_by]).toEqual(["active", "a"])
    await reapUntil(job.id, r => r.state === "pending")
    const b = spawnWorker("b")
    await b.waitFor(completed(job.id))
    const start = b.events().find(e => e.ev === "start" && e.id === job.id)!
    expect(start.attempts).toBe(2)
    expect(await row(job.id))!.toBeUndefined()
  }, 30_000)

  test("inside the claim statement, client_connection_check_interval 100 ms: the backend aborts, nothing is claimed", async () => {
    const job = await enqueue(schema.admin, schema.table, { task: "t" })
    const release = await holdHook(schema.admin, "claim_plain")
    const a = spawnWorker("a", { hooks: ["claim_plain"], connectionCheckMs: 100, poolSize: 1 })
    const ready = await a.waitFor(e => e.ev === "ready")
    await waitForLock(schema.admin, ready.mainPid)
    await a.kill()
    const deadline = Date.now() + 3000
    while ((await backendAlive(ready.mainPid)) && Date.now() < deadline) await sleep(50)
    const terminated = !(await backendAlive(ready.mainPid))
    await release()
    await sleep(200)
    console.log(`connection check: backend terminated before hook release: ${terminated}`)
    expect(terminated).toBe(true)
    expect([(await row(job.id))!.state, (await row(job.id))!.attempts]).toEqual(["pending", 0])
  }, 30_000)

  test("after the claim commit: the handler started and died; the reaper recovers; another worker completes", async () => {
    const job = await enqueue(schema.admin, schema.table, { task: "t" })
    const a = spawnWorker("a", { pauseAt: "after-claim" })
    await a.waitFor(e => e.ev === "paused")
    await a.kill()
    await reapUntil(job.id, r => r.state === "pending")
    const b = spawnWorker("b")
    await b.waitFor(completed(job.id))
    expect(b.events().find(e => e.ev === "start")!.attempts).toBe(2)
    // Accepted duplicate: the job started twice, the first start never finished.
    expect(a.events().filter(e => e.ev === "start")).toHaveLength(1)
  }, 30_000)
})

describe("SIGKILL inside fail and reap", () => {
  test("mid-fail with the competitor locked: the transaction rolls back, both rows intact, competitor unlocked", async () => {
    const job = await enqueue(schema.admin, schema.table, { task: "t", key: "k" })
    const gateFile = join(dir, "gate")
    const a = spawnWorker("a", { failAll: true, pauseAt: "mid-fail", handlerMs: 0, gateFile })
    await a.waitFor(e => e.ev === "start")
    const b = await enqueue(schema.admin, schema.table, {
      task: "t",
      key: "k",
      runAt: new Date(Date.now() + 3600_000),
    })
    writeFileSync(gateFile, "")
    await a.waitFor(e => e.ev === "paused")
    await a.kill()
    await sleep(200)
    expect((await row(job.id))!.state).toBe("active")
    // The competitor is not locked any more.
    await query(
      schema.admin,
      `select id from ${schema.table.name} where id = $1::text::bigint for update nowait`,
      [b.id],
    )
    const reaped = await reapUntil(job.id, r => r.state === "pending")
    expect([reaped.key, (await row(b.id))!.key]).toEqual([null, "k"])
  }, 30_000)

  for (const check of [false, true]) {
    test(`mid-reap, connection check ${check ? "100 ms" : "off"}: all or nothing`, async () => {
      for (let i = 0; i < 20; i++)
        await enqueue(schema.admin, schema.table, { task: "t", key: `k${i}` })
      await claimPlain(schema.admin, schema.table, {
        worker: "dead",
        tasks: ["t"],
        limit: 20,
        defaultLease: 0.1,
      })
      await sleep(200)
      const release = await holdHook(schema.admin, "reap")
      const r = spawnWorker("reaper", {
        role: "reaper",
        hooks: ["reap"],
        poolSize: 1,
        connectionCheckMs: check ? 100 : undefined,
      })
      const ready = await r.waitFor(e => e.ev === "ready")
      await waitForLock(schema.admin, ready.mainPid)
      await r.kill()
      if (check) {
        const deadline = Date.now() + 3000
        while ((await backendAlive(ready.mainPid)) && Date.now() < deadline) await sleep(50)
      }
      await release()
      await sleep(300)
      const states = (await jobs(schema)).map(x => x.state)
      const pending = states.filter(s => s === "pending").length
      console.log(`mid-reap kill, check ${check}: ${pending} of 20 reaped by the dead reaper`)
      expect(pending).toBe(check ? 0 : 20)
      await reap(schema.admin, schema.table)
      expect((await jobs(schema)).every(x => x.state === "pending")).toBe(true)
    }, 30_000)
  }
})

describe("connection cut at COMMIT of fail", () => {
  for (const mode of ["before", "after"] as const) {
    test(`cut ${mode} COMMIT reaches the server: the worker retries on a new connection`, async () => {
      const p = await proxy()
      const job = await enqueue(schema.admin, schema.table, { task: "t" })
      p.proxy.cutNext(/^\s*commit\b/i, mode)
      const a = spawnWorker("a", { mainUrl: p.url, failAll: true, backoff: 3600, handlerMs: 100 })
      const lost = await a.waitFor(e => e.ev === "settle-error")
      expect(lost.phase).toBe("commit")
      const settled = await a.waitFor(e => e.ev === "settled")
      // Before: the retry applies the fail. After: the server applied it, and the retry finds the claim gone.
      expect(settled.outcome).toBe(mode === "before" ? "retry" : "unknown")
      const r = (await row(job.id))!
      expect([r.state, r.attempts, r.last_error]).toEqual(["pending", 1, "injected"])
      await a.kill()
    }, 30_000)
  }
})

describe("heartbeat on an independent connection", () => {
  for (const honorAbort of [true, false]) {
    test(`heartbeat connection lost, handler ${honorAbort ? "honors abort" : "ignores abort (zombie)"}`, async () => {
      const p = await proxy()
      const job = await enqueue(schema.admin, schema.table, { task: "t", queue: "q" })
      const a = spawnWorker("a", { heartbeatUrl: p.url, handlerMs: 3000, honorAbort })
      await a.waitFor(e => e.ev === "start")
      p.proxy.refuse = true
      p.proxy.cut()
      const b = spawnWorker("b", { handlerMs: 300 })
      const reaper = (async () => {
        for (let i = 0; i < 60; i++) {
          await reap(schema.admin, schema.table)
          await sleep(100)
        }
      })()
      await b.waitFor(completed(job.id), 15_000)
      const aEnd = await a.waitFor(e => e.ev === "end", 15_000)
      const bStart = b.events().find(e => e.ev === "start" && e.id === job.id)!
      const overlap = aEnd.t - bStart.t
      if (process.env.FAULT_DEBUG) {
        const all = [...a.events(), ...b.events()].sort((x, y) => x.t - y.t)
        const t0 = all.find(e => e.ev === "start")!.t
        for (const e of all)
          if (e.ev !== "heartbeat") console.log(e.t - t0, e.w, e.ev, e.reason ?? e.message ?? "")
      }
      console.log(
        `heartbeat loss, honorAbort ${honorAbort}: A ended ${overlap > 0 ? `${overlap} ms after` : `${-overlap} ms before`} B started`,
      )
      // A worker that cannot heartbeat must not claim again: every claim it aborts burns an attempt.
      expect(a.events().filter(e => e.ev === "start")).toHaveLength(1)
      expect(bStart.attempts).toBe(2)
      if (honorAbort) {
        // The pool still works, so A fails the job at once instead of leaving it to the reaper.
        expect([aEnd.abort, aEnd.failed]).toEqual(["deadline", true])
        expect(overlap).toBeLessThan(0)
        expect(a.events().find(e => e.ev === "settled")!.outcome).toBe("retry")
      } else {
        // Accepted limitation: a zombie overlaps the next execution, including the queue mutex; its complete is fenced.
        expect(overlap).toBeGreaterThan(0)
        await a.waitFor(e => e.ev === "settled" && e.outcome === "fenced" && e.id === job.id)
      }
      await reaper
    }, 30_000)
  }
})

describe("slots and heartbeats", () => {
  const far = () => new Date(Date.now() + 3600_000)

  test("a slot stays taken until its job is settled", async () => {
    const a = await enqueue(schema.admin, schema.table, { task: "t", key: "k" })
    const gateFile = join(dir, "gate")
    const w = spawnWorker("a", { slots: 1, failAll: true, handlerMs: 0, gateFile, lease: 2 })
    await w.waitFor(e => e.ev === "start" && e.id === a.id)
    // The pending job of the key, held by an open producer transaction, makes the fail of A wait.
    await enqueue(schema.admin, schema.table, { task: "t", key: "k", runAt: far() })
    const producer = await schema.connect()
    await query(producer, "begin")
    await enqueue(producer, schema.table, { task: "t", key: "k", runAt: far() })
    const c = await enqueue(schema.admin, schema.table, { task: "t" })
    writeFileSync(gateFile, "")
    await waitForLockedStatement(schema.admin, "for update")
    await sleep(500)
    const startedEarly = w.events().some(e => e.ev === "start" && e.id === c.id)
    await query(producer, "commit")
    await w.waitFor(e => e.ev === "settled" && e.id === a.id)
    await w.waitFor(e => e.ev === "start" && e.id === c.id)
    expect(startedEarly).toBe(false)
  }, 30_000)

  test("heartbeats run one at a time", async () => {
    await enqueue(schema.admin, schema.table, { task: "t" })
    const release = await holdHook(schema.admin, "heartbeat")
    const w = spawnWorker("a", {
      heartbeatHooks: ["heartbeat"],
      handlerMs: 3000,
      heartbeatMs: 100,
      lease: 2,
    })
    await w.waitFor(e => e.ev === "heartbeat")
    await sleep(600)
    const sent = w.events().filter(e => e.ev === "heartbeat").length
    await release()
    expect(sent).toBe(1)
  }, 30_000)

  test("a worker whose heartbeat is stuck does not claim", async () => {
    await enqueue(schema.admin, schema.table, { task: "t" })
    const release = await holdHook(schema.admin, "heartbeat")
    const w = spawnWorker("a", {
      heartbeatHooks: ["heartbeat"],
      handlerMs: 5000,
      heartbeatMs: 100,
      lease: 2,
    })
    await w.waitFor(e => e.ev === "heartbeat")
    await sleep(300)
    const late = await enqueue(schema.admin, schema.table, { task: "t" })
    await sleep(500)
    const claimed = w.events().some(e => e.ev === "start" && e.id === late.id)
    await release()
    await w.waitFor(e => e.ev === "start" && e.id === late.id)
    expect(claimed).toBe(false)
  }, 30_000)

  test("a zombie of a reclaimed job keeps its slot and cannot settle the new claim", async () => {
    const job = await enqueue(schema.admin, schema.table, { task: "t" })
    const w = spawnWorker("a", { slots: 2, handlerMs: 2500, honorAbort: false })
    const first = await w.waitFor(e => e.ev === "start" && e.id === job.id)
    // The reaper returns the job while its handler runs.
    await query(schema.admin, "begin")
    await query(
      schema.admin,
      `update ${schema.table.name} set lease_until = now() - interval '1 second' where id = $1::text::bigint`,
      [job.id],
    )
    await reap(schema.admin, schema.table)
    await query(schema.admin, "commit")
    const second = await w.waitFor(e => e.ev === "start" && e.id === job.id && e.attempts === 2)
    const other = await enqueue(schema.admin, schema.table, { task: "t" })
    await sleep(500)
    const otherEarly = w.events().some(e => e.ev === "start" && e.id === other.id)
    await w.waitFor(e => completed(job.id)(e) && e.claimId === second.claimId, 15_000)
    await w.waitFor(e => e.ev === "start" && e.id === other.id, 15_000)
    expect(otherEarly).toBe(false)
    expect(w.events().find(e => e.ev === "settled" && e.claimId === first.claimId)!.outcome).toBe(
      "lost",
    )
  }, 30_000)
})

describe("long producer transaction", () => {
  test("holding a due row delays only that row; claim lag shows the delay", async () => {
    const held = await enqueue(schema.admin, schema.table, { task: "t", key: "held" })
    const producer = await schema.connect()
    await query(producer, "begin")
    await enqueue(producer, schema.table, { task: "t", key: "held", payload: "event" })
    const a = spawnWorker("a")
    const other = await enqueue(schema.admin, schema.table, { task: "t" })
    const otherStart = await a.waitFor(e => e.ev === "start" && e.id === other.id)
    await sleep(2000)
    expect(a.events().some(e => e.ev === "start" && e.id === held.id)).toBe(false)
    await query(producer, "commit")
    const heldStart = await a.waitFor(e => e.ev === "start" && e.id === held.id)
    console.log(
      `long producer: other job lag ${otherStart.lag.toFixed(3)} s, held job lag ${heldStart.lag.toFixed(3)} s`,
    )
    expect(otherStart.lag).toBeLessThan(0.5)
    expect(heldStart.lag).toBeGreaterThan(1.8)
  }, 30_000)
})

describe("stopOnSignals", () => {
  test("a handler that honors the abort: one stop for both signals, the process exits with 0, and the job returns without spending its last attempt", async () => {
    const job = await enqueue(schema.admin, schema.table, { task: "t", maxAttempts: 1 })
    const abortGateFile = join(dir, "abort-gate")
    const a = spawnWorker("a", {
      handlerMs: 60_000,
      abortGateFile,
      shutdown: { abortAfter: 0, timeout: 10 },
    })
    await a.waitFor(e => e.ev === "start")
    a.proc.kill("SIGTERM")
    await a.waitFor(e => e.ev === "signal")
    a.proc.kill("SIGINT")
    await a.waitFor(e => e.ev === "signal" && e.signal === "SIGINT")
    // The handler ends on the abort once both signals came.
    writeFileSync(abortGateFile, "")
    expect(await a.proc.exited).toBe(0)
    expect(a.events().filter(e => ["signal", "close", "settled"].includes(e.ev))).toEqual([
      expect.objectContaining({ ev: "signal", signal: "SIGTERM" }),
      expect.objectContaining({ ev: "signal", signal: "SIGINT" }),
      expect.objectContaining({ ev: "settled", outcome: "requeued" }),
      expect.objectContaining({ ev: "close" }),
    ])
    const r = (await row(job.id))!
    expect([r.state, r.attempts]).toEqual(["pending", 0])
  }, 30_000)

  test("a handler that ignores the abort: the process exits with 1 at the timeout, and the job returns after its lease with the attempt spent", async () => {
    const job = await enqueue(schema.admin, schema.table, { task: "t" })
    const a = spawnWorker("a", {
      handlerMs: 60_000,
      honorAbort: false,
      shutdown: { abortAfter: 0, timeout: 0.5 },
    })
    await a.waitFor(e => e.ev === "start")
    const signalAt = Date.now()
    a.proc.kill("SIGTERM")
    expect(await a.proc.exited).toBe(1)
    expect(Date.now() - signalAt).toBeLessThan(3000)
    expect(a.events().filter(e => e.ev === "close")).toEqual([])
    expect((await row(job.id))!.state).toBe("active")
    const reaped = await reapUntil(job.id, r => r.state === "pending")
    expect(reaped.attempts).toBe(1)
  }, 30_000)
})

describe("fairness across processes under continuous inflow", () => {
  test("3 workers × 2 slots, inflow above capacity for plain jobs and 20 queues: no class or queue starves", async () => {
    const workers = ["a", "b", "c"].map(id => spawnWorker(id, { handlerMs: 20, pollMs: 5 }))
    await Promise.all(workers.map(w => w.waitFor(e => e.ev === "ready")))
    const stopAt = Date.now() + 5000
    while (Date.now() < stopAt) {
      // About 200 plain jobs/s and 400 queued jobs/s against a capacity of about 300 jobs/s.
      await query(
        schema.admin,
        `insert into ${schema.table.name} (task, queue, payload, run_at, window_started_at, created_at, max_attempts)
         select 't', q, '{}', now(), now(), now(), 25
         from unnest(array[null, null] || array(select 'q' || lpad(i::text, 2, '0') from generate_series(0, 19) i
                                                where random() < 0.2)) q`,
      )
      await sleep(10)
    }
    for (const w of workers) await w.kill()
    const starts = workers.flatMap(w => w.events()).filter(e => e.ev === "start")
    const plain = starts.filter(e => e.queue == null)
    const queued = starts.filter(e => e.queue != null)
    const perQueue = new Map<string, Event[]>()
    for (const e of queued) perQueue.set(e.queue, [...(perQueue.get(e.queue) ?? []), e])
    const counts = [...perQueue.values()].map(xs => xs.length)
    const maxLag = (xs: Event[]) => Math.max(...xs.map(e => e.lag))
    console.log(
      `fairness across processes: ${starts.length} starts, plain ${plain.length}, queued ${queued.length}, ` +
        `queues ${perQueue.size}, per queue min ${Math.min(...counts)} max ${Math.max(...counts)}, ` +
        `max lag plain ${maxLag(plain).toFixed(2)} s, queued ${maxLag(queued).toFixed(2)} s`,
    )
    expect(plain.length / starts.length).toBeGreaterThan(0.3)
    expect(queued.length / starts.length).toBeGreaterThan(0.3)
    expect(perQueue.size).toBe(20)
    expect(Math.min(...counts)).toBeGreaterThanOrEqual(Math.max(...counts) / 2)
  }, 30_000)
})
