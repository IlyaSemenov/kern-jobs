// Randomized real-time run of worker runtimes: concurrent producers in transactions with domain writes, four workers whose handlers fail, run long, or ignore the abort, and faults injected into the workers: terminated backends of their pools and heartbeat connections, and heartbeat partitions longer than a lease.
// Checks, as handlers and settlement outcomes show them:
// - the last domain write of every key was observed by a handler that returned and whose settlement outcome is `completed` or `unknown`;
// - no handler of a job or of a queue starts while another handler of it runs whose signal is not aborted;
// - no job gets the outcome `completed` twice.
import { afterEach, beforeEach, expect, test } from "bun:test"

import type { Driver } from "kern-db"

import { enqueue } from "../core/enqueue"
import type { KeyMode } from "../core/job"
import { query } from "../core/statement"
import {
  type Conn,
  createRandom,
  createTestDriver,
  createTestSchema,
  databaseUrl,
  sleep,
  type TestSchema,
} from "../core/testing"

import type { WorkerEvent } from "./events"
import { type Proxy, proxyTo } from "./faults/proxy"
import { startRuntime, type Worker, type WorkerTask } from "./runtime"
import { terminateBackends } from "./testing"

let schema: TestSchema
beforeEach(async () => {
  schema = await createTestSchema()
})
afterEach(async () => {
  await schema.dispose()
})

const SEED = Number(process.env.FUZZ_SEED ?? 1)
const ROUNDS = Number(process.env.FUZZ_ROUNDS ?? 20)
const ROUND_MS = Number(process.env.FUZZ_ROUND_MS ?? 200)
const LEASE = 0.5

const KEYS = Array.from({ length: 9 }, (_, i) => `k${i}`)
const modeOf = (key: string): KeyMode =>
  (["replace", "keep", "debounce"] as const)[Number(key.slice(1)) % 3]!
const DEBOUNCE = { delay: 0.03, maxWait: 0.15 }

/** Waits `ms`, or rejects with the reason of the signal when it aborts first. */
function pause(ms: number, signal: AbortSignal | null): Promise<void> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(resolve, ms)
    signal?.addEventListener("abort", () => {
      clearTimeout(timer)
      reject(signal.reason)
    })
  })
}

test(`fuzz seed ${SEED}`, async () => {
  const random = createRandom(SEED)
  await query(
    schema.admin,
    `create table ${schema.name}.data (key text primary key, version int not null default 0)`,
  )
  await query(
    schema.admin,
    `insert into ${schema.name}.data (key) select unnest($1::text::text[])`,
    [KEYS],
  )

  const seen = new Map<string, number>()
  const stats = {
    events: 0,
    rolledBack: 0,
    queueJobs: 0,
    queueDone: 0,
    outcomes: {} as Record<string, number>,
    aborts: {} as Record<string, number>,
    terminated: 0,
    partitions: 0,
    claimErrors: 0,
    settleErrors: 0,
    heartbeatErrors: 0,
    reaped: 0,
    zombies: 0,
    jobOverlaps: 0,
    doubleCompletions: 0,
    queueOverlaps: 0,
  }
  const count = (o: Record<string, number>, k: string) => (o[k] = (o[k] ?? 0) + 1)
  // Signals of the handlers in flight by job id and by queue; one that is not aborted makes an overlap a violation.
  const executing = new Map<string, Set<AbortSignal>>()
  const queueRunning = new Map<string, Set<AbortSignal>>()
  const observed = new Map<string, { key: string | null; version: number; queue: string | null }>()
  const completedIds = new Set<string>()
  const doneQueueIds = new Set<string>()
  let inFlight = 0
  let unexpected: unknown
  let producing = false
  let faults = false
  const lost: { round: number; key: string; seen: number; version: number }[] = []
  const producerConns = await Promise.all(Array.from({ length: 4 }, () => schema.connect()))

  const produce = (c: Conn) => async () => {
    while (producing) {
      const queued = random.float() < 0.2
      await query(c, "begin")
      if (queued) {
        await enqueue(c, schema.table, {
          task: "q",
          queue: `q${random.int(0, 2)}`,
          maxAttempts: 50,
        })
      } else {
        const key = KEYS[random.int(0, KEYS.length - 1)]!
        await query(
          c,
          `update ${schema.name}.data set version = version + 1 where key = $1::text`,
          [key],
        )
        // Throttle-like run times in the near future keep the pending row alive while events arrive.
        const mode = modeOf(key)
        const runAt =
          mode === "debounce" ? null : new Date(Date.now() + (mode === "keep" ? 30 : 20))
        await enqueue(c, schema.table, {
          task: "t",
          key,
          mode,
          runAt,
          ...DEBOUNCE,
          payload: key,
          maxAttempts: 50,
        })
      }
      await sleep(random.float() * 8)
      if (random.float() < 0.1) {
        await query(c, "rollback")
        stats.rolledBack++
      } else {
        await query(c, "commit")
        if (queued) stats.queueJobs++
        else stats.events++
      }
      await sleep(random.float() * 3)
    }
  }

  const task: WorkerTask = {
    async handler(job, { signal }) {
      const zombie = faults && random.float() < 0.2
      if (zombie) stats.zombies++
      const others = executing.get(job.id) ?? new Set()
      for (const s of others) if (!s.aborted) stats.jobOverlaps++
      others.add(signal)
      executing.set(job.id, others)
      const queue = job.queue ? (queueRunning.get(job.queue) ?? new Set()) : null
      if (queue) {
        for (const s of queue) if (!s.aborted) stats.queueOverlaps++
        queue.add(signal)
        queueRunning.set(job.queue!, queue)
      }
      try {
        const version = job.key
          ? Number(
              (
                await query(
                  schema.driver,
                  `select version from ${schema.name}.data where key = $1::text`,
                  [job.key],
                )
              )[0]!.version,
            )
          : 0
        const long = faults && random.float() < 0.1
        await pause(long ? LEASE * 1400 : random.float() * 15, zombie ? null : signal)
        if (faults && random.float() < 0.12) throw new Error("boom")
        observed.set(job.claim_id!, { key: job.key, version, queue: job.queue })
      } finally {
        others.delete(signal)
        queue?.delete(signal)
      }
    },
    backoff: () => 0.02,
  }

  const onEvent = (e: WorkerEvent) => {
    switch (e.type) {
      case "start":
        inFlight++
        return
      case "settled": {
        inFlight--
        count(stats.outcomes, e.outcome)
        const o = observed.get(e.claim.claimId)
        if (e.outcome === "completed") {
          if (completedIds.has(e.claim.id)) stats.doubleCompletions++
          completedIds.add(e.claim.id)
        }
        if (o && (e.outcome === "completed" || e.outcome === "unknown")) {
          if (o.key) seen.set(o.key, Math.max(seen.get(o.key) ?? 0, o.version))
          if (o.queue) doneQueueIds.add(e.claim.id)
        }
        return
      }
      case "abort":
        count(stats.aborts, e.reason)
        return
      case "claim-error":
        stats.claimErrors++
        return
      case "settle-error":
        stats.settleErrors++
        return
      case "heartbeat-error":
        stats.heartbeatErrors++
        return
      case "reap":
        stats.reaped += e.jobs.length
        return
      case "error":
        // The worker ignores an error that onEvent throws, so the drain loop rethrows it.
        unexpected ??= e.error
        return
    }
  }

  const drivers: Driver[] = []
  const proxies: Proxy[] = []
  const workers: Worker[] = []
  for (let i = 0; i < 4; i++) {
    const { proxy, url } = await proxyTo(databaseUrl())
    proxies.push(proxy)
    const driver = createTestDriver(databaseUrl(), schema.name, {
      max: 5,
      applicationName: `w${i}`,
    })
    const heartbeatSource = createTestDriver(url, schema.name, {
      max: 1,
      applicationName: `w${i}-heartbeat`,
    })
    drivers.push(driver, heartbeatSource)
    workers.push(
      startRuntime({
        id: `w${i}`,
        driver,
        table: schema.table,
        heartbeatSource,
        tasks: { t: task, q: task },
        concurrency: 3,
        lease: LEASE,
        heartbeatInterval: 0.1,
        abortMargin: 0.15,
        pollInterval: 0.005,
        reaper: { interval: 0.05, limit: 100 },
        onEvent,
      }),
    )
  }

  // Faults while producers run: terminated backends of the pools and heartbeat connections of workers, and heartbeat partitions longer than a lease, after which the other workers reap the jobs of the partitioned worker.
  let done = false
  const injector = (async () => {
    while (!done) {
      await sleep(30)
      if (!faults) continue
      const w = random.int(0, 3)
      const roll = random.float()
      if (roll < 0.15) {
        stats.terminated += await terminateBackends(schema.admin, `w${w}`)
      } else if (roll < 0.3) {
        stats.terminated += await terminateBackends(schema.admin, `w${w}-heartbeat`)
      } else if (roll < 0.4) {
        const proxy = proxies[w]!
        if (proxy.refuse) continue
        stats.partitions++
        proxy.refuse = true
        proxy.cut()
        setTimeout(() => (proxy.refuse = false), LEASE * 1500)
      }
    }
  })()

  const idle = async () =>
    (
      await query(
        schema.admin,
        `select count(*)::int as n from ${schema.table.name} where state in ('pending', 'active')`,
      )
    )[0]!.n === "0"

  // Each round: a burst of events with faults, then a drain without faults, then the check.
  // The last event of each key in a round has no later event to mask a lost trigger.
  for (let round = 0; round < ROUNDS; round++) {
    producing = true
    faults = true
    const producers = producerConns.map(c => produce(c)())
    await sleep(ROUND_MS)
    producing = false
    await Promise.all(producers)
    faults = false
    while (!(inFlight === 0 && (await idle()))) {
      if (unexpected) throw unexpected
      await sleep(10)
    }
    const finals = await query(
      schema.admin,
      `select key, version from ${schema.name}.data order by key`,
    )
    for (const row of finals) {
      const [key, version] = [row.key!, Number(row.version)]
      const s = seen.get(key) ?? 0
      if (s !== version) lost.push({ round, key, seen: s, version })
    }
  }
  done = true
  await injector
  await Promise.all(workers.map(w => w.stop()))
  await Promise.all(drivers.map(d => d.close()))
  await Promise.all(proxies.map(p => p.close()))
  stats.queueDone = doneQueueIds.size

  console.log(`fuzz seed ${SEED}:`, JSON.stringify(stats))
  expect(lost).toEqual([])
  expect([stats.jobOverlaps, stats.doubleCompletions, stats.queueOverlaps]).toEqual([0, 0, 0])
  expect(stats.queueDone).toBe(stats.queueJobs)
  expect(stats.outcomes.error ?? 0).toBe(0)
  expect(
    (await query(schema.admin, `select count(*)::int as n from ${schema.table.name}`))[0]!.n,
  ).toBe("0")
}, 180_000)
