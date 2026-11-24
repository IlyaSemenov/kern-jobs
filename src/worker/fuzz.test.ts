// Randomized real-time run of worker runtimes: concurrent producers in transactions with domain writes, four workers whose handlers fail, run long, or ignore the abort, and faults injected into the workers: terminated backends of their pools and heartbeat connections, and heartbeat partitions longer than a lease.
// Doomed jobs fail for good with a failure task, by their handlers or by the faults, and their failure jobs fail on their own while the faults run.
// Checks, as handlers and settlement outcomes show them:
// - the last domain write of every key was observed by a handler that returned and whose settlement outcome is `completed` or `unknown`;
// - no handler of a job or of a queue starts while another handler of it runs whose signal is not aborted;
// - no job gets the outcome `completed` twice.
// Checks of failure jobs, by a ledger that triggers write in the transactions of the job table, since a lost COMMIT hides an outcome from the worker:
// - each committed failure for good of a claim with a stamp inserted exactly one failure job, and each failure job names such a failure;
// - each failure handler that ran named a committed failure;
// - after the drain, each failure job completed after a handler of it returned, or failed for good by its own budget.
import { afterEach, beforeEach, expect, test } from "bun:test"

import type { Driver } from "kern-orm/db"

import { enqueue } from "../sql/enqueue"
import type { KeyMode } from "../sql/job"
import { TerminalError } from "../task/errors"
import type { JobFailure } from "../task/failure"
import {
  type Conn,
  createTestDriver,
  createTestSchema,
  databaseUrl,
  sleep,
  type TestSchema,
} from "../test-harness/database"
import { createRandom } from "../test-harness/random"

import { query } from "./db/query"
import type { WorkerEvent } from "./events"
import { type Proxy, proxyTo } from "./faults/proxy"
import { startRuntime, type Worker, type WorkerTask } from "./runtime"
import { terminateBackends } from "./test-harness"

let schema: TestSchema
/** Stops the faults and the workers of the test and closes their connections before its schema is dropped, also after a test that failed or timed out. */
let cleanUp: (() => Promise<void>) | undefined
beforeEach(async () => {
  schema = await createTestSchema()
})
afterEach(async () => {
  await cleanUp?.()
  cleanUp = undefined
  await schema.dispose()
})

const SEED = Number(process.env.FUZZ_SEED ?? 1)
const ROUNDS = Number(process.env.FUZZ_ROUNDS ?? 20)
const ROUND_MS = Number(process.env.FUZZ_ROUND_MS ?? 200)
const LEASE = 0.5
// A round fails with a report of its state when its drain takes longer.
const DRAIN_MS = 30_000

const KEYS = Array.from({ length: 9 }, (_, i) => `k${i}`)
const modeOf = (key: string): KeyMode =>
  (["replace", "keep", "debounce"] as const)[Number(key.slice(1)) % 3]!
const DEBOUNCE = { delay: 0.03, maxWait: 0.15 }

/** Failure task of the doomed task `d`. */
const FAILURE_TASK = "d:failure"
/** Attempt budget of the failure jobs, which the faults of a run are unlikely to spend. */
const FAILURE_MAX_ATTEMPTS = 50

/**
 * Creates the ledger of the failure jobs in a test schema: triggers record each transition of a row with a failure task from active to failed and each insert of a failure job in the transaction that makes it, so the ledger holds what committed.
 */
async function createLedger(conn: Conn, schemaName: string, table: string) {
  await conn.executeScript(`
    create table ${schemaName}.terminal_ledger (claim_id bigint not null, job_id bigint not null, last_error text);
    create table ${schemaName}.failure_ledger (failure_id bigint not null, job_id bigint not null, reason text not null);
    create function ${schemaName}.record_terminal() returns trigger language plpgsql as $$
    begin
      insert into ${schemaName}.terminal_ledger values (old.claim_id, old.id, new.last_error);
      return null;
    end $$;
    create trigger record_terminal after update on ${table} for each row
      when (old.state = 'active' and new.state = 'failed' and old.failure_max_attempts is not null)
      execute function ${schemaName}.record_terminal();
    create function ${schemaName}.record_failure() returns trigger language plpgsql as $$
    begin
      insert into ${schemaName}.failure_ledger values ((new.payload ->> 'id')::bigint, new.id, new.payload ->> 'reason');
      return null;
    end $$;
    create trigger record_failure after insert on ${table} for each row
      when (new.task = '${FAILURE_TASK}') execute function ${schemaName}.record_failure();
  `)
}

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
  await createLedger(schema.admin, schema.name, schema.table.name)

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
    lateClaims: 0,
    zombies: 0,
    jobOverlaps: 0,
    doubleCompletions: 0,
    queueOverlaps: 0,
    doomed: 0,
    failureRuns: 0,
    failureFaults: 0,
  }
  const count = (o: Record<string, number>, k: string) => (o[k] = (o[k] ?? 0) + 1)
  // Signals of the handlers in flight by job id and by queue; one that is not aborted makes an overlap a violation.
  const executing = new Map<string, Set<AbortSignal>>()
  const queueRunning = new Map<string, Set<AbortSignal>>()
  const observed = new Map<string, { key: string | null; version: number; queue: string | null }>()
  const completedIds = new Set<string>()
  const doneQueueIds = new Set<string>()
  // Claims whose handler started and whose settlement has not ended; a late claim settles without a start, so a counter would go below zero.
  const inFlight = new Set<string>()
  let unexpected: unknown
  let producing = false
  let faults = false
  const lost: { round: number; key: string; seen: number; version: number }[] = []
  const producerConns = await Promise.all(Array.from({ length: 4 }, () => schema.connect()))
  // Failures named by the failure handlers that started, and failure jobs whose handler returned.
  const failureRuns = new Set<string>()
  const failureReturned = new Set<string>()

  const produce = (c: Conn) => async () => {
    while (producing) {
      const roll = random.float()
      const queued = roll < 0.2
      const doomed = !queued && roll < 0.3
      await query(c, "begin")
      if (doomed) {
        // A budget of one or two attempts, so that handlers and faults fail them for good.
        await enqueue(c, schema.table, { task: "d", maxAttempts: random.int(1, 2) })
      } else if (queued) {
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
        if (doomed) stats.doomed++
        else if (queued) stats.queueJobs++
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
        observed.set(job.claimId!, { key: job.key, version, queue: job.queue })
      } finally {
        others.delete(signal)
        queue?.delete(signal)
      }
    },
    backoff: () => 0.02,
  }

  const doomedTask: WorkerTask = {
    async handler(_, { signal }) {
      await pause(random.float() * 10, signal)
      throw random.float() < 0.3 ? new TerminalError("doomed") : new Error("doomed")
    },
    backoff: () => 0.02,
    failureMaxAttempts: FAILURE_MAX_ATTEMPTS,
  }

  // Fails on its own while the faults run, and sometimes outlives its lease.
  const failureTask: WorkerTask = {
    async handler(job, { signal }) {
      failureRuns.add((job.payload as JobFailure).id)
      stats.failureRuns++
      const long = faults && random.float() < 0.05
      await pause(long ? LEASE * 1400 : random.float() * 10, signal)
      if (faults && random.float() < 0.3) {
        stats.failureFaults++
        throw new Error("cleanup failed")
      }
      failureReturned.add(job.id)
    },
    backoff: () => 0.02,
  }

  const onEvent = (e: WorkerEvent) => {
    switch (e.type) {
      case "start":
        inFlight.add(e.claim.claimId)
        return
      case "settled": {
        inFlight.delete(e.claim.claimId)
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
      case "late-claim":
        stats.lateClaims++
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
        tasks: { t: task, q: task, d: doomedTask, [FAILURE_TASK]: failureTask },
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

  /** Returns a JSON snapshot of the claims in flight, the states of the workers, and the unfinished jobs. */
  const describeDrain = async () => {
    const jobs = await query(
      schema.admin,
      `select id, state, task, queue, key, attempts, claim_id, locked_by,
         run_at - now() as run_in, not_before - now() as not_before_in, lease_until - now() as lease_left
       from ${schema.table.name} where state in ('pending', 'active') order by id`,
    )
    return JSON.stringify({ inFlight: [...inFlight], workers: workers.map(w => w.state), jobs })
  }

  // The workers get 3 seconds to stop and their connections to close, since a pool with a hung statement does not close, and the schema must still be dropped; the closing may then go on in the background.
  cleanUp = async () => {
    done = true
    faults = false
    await Promise.race([
      (async () => {
        await Promise.allSettled(workers.map(w => w.stop({ abortAfter: 0, timeout: 1 })))
        await Promise.allSettled([...drivers.map(d => d.close()), ...proxies.map(p => p.close())])
      })(),
      sleep(3000),
    ])
  }

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
    const drainBy = performance.now() + DRAIN_MS
    // The query runs before the look at `inFlight`: with no unfinished job, no further handler starts, so an empty `inFlight` then means that every `settled` has arrived.
    while (!((await idle()) && inFlight.size === 0)) {
      if (unexpected) throw unexpected
      if (performance.now() > drainBy) {
        throw new Error(
          `fuzz seed ${SEED}: round ${round} did not drain in ${DRAIN_MS} ms: ${await describeDrain()}`,
        )
      }
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
  // The connections of the workers are closed by `cleanUp`.
  await Promise.all(workers.map(w => w.stop()))
  stats.queueDone = doneQueueIds.size

  const s = schema.name
  const ledger = await query(
    schema.admin,
    `select
       (select count(*) from ${s}.terminal_ledger)::text as terminal,
       (select count(*) from ${s}.failure_ledger)::text as failure,
       (select count(*) from ${schema.table.name} where task = '${FAILURE_TASK}' and state = 'failed')::text as exhausted`,
  )
  console.log(`fuzz seed ${SEED}:`, JSON.stringify({ ...stats, ledger: ledger[0] }))
  expect(lost).toEqual([])
  expect([stats.jobOverlaps, stats.doubleCompletions, stats.queueOverlaps]).toEqual([0, 0, 0])
  expect(stats.queueDone).toBe(stats.queueJobs)
  expect(stats.outcomes.error ?? 0).toBe(0)
  // Only doomed jobs and failure jobs that spent their budgets stay, as failed.
  expect(
    await query(
      schema.admin,
      `select task, state from ${schema.table.name}
       where not (state = 'failed' and task in ('d', '${FAILURE_TASK}')) order by id`,
    ),
  ).toEqual([])
  // Each doomed job has no key and a handler that always throws, so each committed one failed for good once, with the stamp of every worker.
  expect(stats.doomed).toBeGreaterThan(0)
  expect(Number(ledger[0]!.terminal)).toBe(stats.doomed)
  // Each committed failure for good of a claim with a stamp inserted exactly one failure job of that failure, and no failure job names another.
  expect(
    await query(
      schema.admin,
      `select t.claim_id::text, count(f.job_id)::text as failure_jobs
       from ${s}.terminal_ledger t left join ${s}.failure_ledger f on f.failure_id = t.claim_id
       group by t.claim_id having count(f.job_id) <> 1 or count(distinct t.job_id) <> 1 or count(*) <> count(f.job_id)`,
    ),
  ).toEqual([])
  expect(
    await query(
      schema.admin,
      `select f.failure_id::text from ${s}.failure_ledger f
       where not exists (select from ${s}.terminal_ledger t where t.claim_id = f.failure_id)`,
    ),
  ).toEqual([])
  // The reason of a failure that the reaper recorded agrees with the error of the failed row.
  expect(
    await query(
      schema.admin,
      `select f.failure_id::text, f.reason, t.last_error from ${s}.failure_ledger f
       join ${s}.terminal_ledger t on t.claim_id = f.failure_id
       where (f.reason = 'lease-expired') <> (t.last_error = 'lease expired')`,
    ),
  ).toEqual([])
  // Each failure handler that ran named a committed failure.
  const committed = new Set(
    (await query(schema.admin, `select claim_id::text as id from ${s}.terminal_ledger`)).map(
      r => r.id!,
    ),
  )
  expect([...failureRuns].filter(id => !committed.has(id))).toEqual([])
  // A failure job that is gone completed, which follows a handler of it that returned; any other failed for good by its own budget.
  const gone = await query(
    schema.admin,
    `select f.job_id::text as id from ${s}.failure_ledger f
     where not exists (select from ${schema.table.name} j where j.id = f.job_id)`,
  )
  expect(gone.map(r => r.id!).filter(id => !failureReturned.has(id))).toEqual([])
}, 180_000)
