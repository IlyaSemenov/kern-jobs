// Claiming: the tasks of the worker, the queue mutex by (scope, queue), start order, skipping a busy head, a stale snapshot under a breakpoint, a stress test that checks the actual overlap of handlers, fairness of the claim cycle under a continuous inflow, claims committed before a later statement of their cycle fails, and the advisory locks of claimQueued under different plans.
import { afterEach, beforeEach, describe, expect, test } from "bun:test"

import { ConnectionError } from "kern-db"
import { waitForLock } from "kern-db/testing"

import { claimCycle, type ClaimCycleStats, claimQueued, createClaimState } from "./claim"
import { enqueue } from "./enqueue"
import type { Claim } from "./job"
import { complete } from "./settlement"
import { query, type StatementTarget } from "./statement"
import {
  type Conn,
  createRandom,
  createTestSchema,
  holdHook,
  isPending,
  jobs,
  only,
  secondsOf,
  sleep,
  T,
  type TestSchema,
} from "./testing"
import { isUniqueViolation } from "./violation"

let schema: TestSchema
beforeEach(async () => {
  schema = await createTestSchema()
})
afterEach(async () => {
  await schema.dispose()
})

const W = (worker: string) => ({ worker, tasks: ["t"] })

describe("tasks of the worker", () => {
  test("a worker claims only jobs of its tasks, and a queue starts its earliest job of them, also behind a job of another task", async () => {
    const plainB = await enqueue(schema.admin, schema.table, { task: "b", now: T(0) })
    // A queue with jobs of task b only, which the scan visits first, must not end the claims of task a.
    const onlyB = await enqueue(schema.admin, schema.table, { task: "b", queue: "p", now: T(0) })
    const queuedB = await enqueue(schema.admin, schema.table, { task: "b", queue: "q", now: T(0) })
    const plainA = await enqueue(schema.admin, schema.table, { task: "a", now: T(1) })
    const queuedA = await enqueue(schema.admin, schema.table, { task: "a", queue: "q", now: T(1) })
    const a = await claimCycle(schema.admin, schema.table, {
      worker: "a",
      tasks: ["a"],
      slots: 4,
      now: T(2),
    })
    const b = await claimCycle(schema.admin, schema.table, {
      worker: "b",
      tasks: ["b"],
      slots: 4,
      now: T(2),
    })
    // The queue is busy with the job of task a, so its earlier job of task b waits.
    expect([a.map(c => c.id).sort(), b.map(c => c.id).sort()]).toEqual([
      [plainA.id, queuedA.id].sort(),
      [plainB.id, onlyB.id].sort(),
    ])
    expect(only(await jobs(schema, `id = ${queuedB.id}`)).state).toBe("pending")
  })
})

describe("queue mutex", () => {
  test("two active jobs in one (scope, queue) are structurally impossible", async () => {
    await enqueue(schema.admin, schema.table, { task: "t", queue: "q", now: T(0) })
    await enqueue(schema.admin, schema.table, { task: "t", queue: "q", now: T(0) })
    const err = await query(
      schema.admin,
      `update ${schema.table.name} set state = 'active', claim_id = id, lease_until = now()`,
    ).catch(e => e)
    expect(isUniqueViolation(err, "job_active_queue_idx")).toBe(true)
  })

  // Graphile Worker ran jobs of one named queue concurrently with its batch fetch, `localQueue`: https://github.com/graphile/worker/issues/621
  test("one worker with 4 slots and 4 ready jobs of one queue: one active", async () => {
    for (let i = 0; i < 4; i++)
      await enqueue(schema.admin, schema.table, { task: "t", queue: "q", now: T(i) })
    const claims = await claimCycle(schema.admin, schema.table, {
      ...W("w1"),
      slots: 4,
      now: T(10),
    })
    expect(claims).toHaveLength(1)
    expect((await jobs(schema)).map(r => r.state)).toEqual([
      "active",
      "pending",
      "pending",
      "pending",
    ])
  })

  test("one queue name in different scopes is not serialized; jobs without a queue are claimed in a batch", async () => {
    await enqueue(schema.admin, schema.table, { task: "t", queue: "q", scope: "a", now: T(0) })
    await enqueue(schema.admin, schema.table, { task: "t", queue: "q", scope: "b", now: T(0) })
    for (let i = 0; i < 3; i++) await enqueue(schema.admin, schema.table, { task: "t", now: T(0) })
    const claims = await claimCycle(schema.admin, schema.table, {
      ...W("w1"),
      slots: 10,
      now: T(1),
    })
    expect(claims).toHaveLength(5)
  })
})

describe("start order", () => {
  test("interleaving of A and B: a busy queue does not wait, and J2 starts only after J1 completes", async () => {
    const j1 = await enqueue(schema.admin, schema.table, {
      task: "t",
      queue: "q",
      payload: 1,
      now: T(1),
    })
    const j2 = await enqueue(schema.admin, schema.table, {
      task: "t",
      queue: "q",
      payload: 2,
      now: T(2),
    })
    const a = await schema.connect()
    const b = await schema.connect()
    await query(a, "begin")
    const ra = await claimQueued(a, schema.table, { ...W("a"), now: T(5) })
    expect(ra.kind === "job" && ra.claim.id).toBe(j1.id)
    // B does not block: the uncommitted claim of A holds the advisory lock of the queue.
    const rb = claimQueued(b, schema.table, { ...W("b"), now: T(5) })
    expect(await isPending(rb, 300)).toBe(false)
    expect((await rb).kind).toBe("none")
    await query(a, "commit")
    expect((await claimQueued(b, schema.table, { ...W("b"), now: T(5) })).kind).toBe("none")
    if (ra.kind !== "job") throw new Error()
    expect(await complete(a, schema.table, ra.claim)).toBe(true)
    const rb2 = await claimQueued(b, schema.table, { ...W("b"), now: T(5) })
    expect(rb2.kind === "job" && rb2.claim.id).toBe(j2.id)
  })

  test("a producer holds the head: the queue is skipped as busy, J2 is not claimed, and nothing waits", async () => {
    await enqueue(schema.admin, schema.table, {
      task: "t",
      queue: "q",
      key: "k1",
      payload: 1,
      now: T(1),
    })
    await enqueue(schema.admin, schema.table, {
      task: "t",
      queue: "q",
      key: "k2",
      payload: 2,
      now: T(2),
    })
    const producer = await schema.connect()
    await query(producer, "begin")
    await enqueue(producer, schema.table, {
      task: "t",
      queue: "q",
      key: "k1",
      payload: 11,
      runAt: T(1),
      now: T(3),
    })
    const r = claimQueued(schema.admin, schema.table, { ...W("a"), now: T(5) })
    expect(await isPending(r, 300)).toBe(false)
    expect(await r).toEqual({ kind: "busy", queue: { scope: "", queue: "q" } })
    expect(
      await claimCycle(schema.admin, schema.table, { ...W("a"), slots: 4, now: T(5) }),
    ).toEqual([])
    await query(producer, "commit")
    const claim = only(
      await claimCycle(schema.admin, schema.table, { ...W("a"), slots: 4, now: T(5) }),
    )
    expect(claim.job.payload).toBe(11)
  })

  test("a head that a producer moved after the claim snapshot does not start early", async () => {
    await enqueue(schema.admin, schema.table, {
      task: "t",
      queue: "q",
      key: "k1",
      payload: 1,
      now: T(1),
    })
    await enqueue(schema.admin, schema.table, {
      task: "t",
      queue: "q",
      key: "k2",
      payload: 2,
      now: T(2),
    })
    const worker = await schema.connect({ hooks: ["claim_queue"] })
    const release = await holdHook(schema.admin, "claim_queue")
    const claim = claimQueued(worker, schema.table, { ...W("a"), now: T(5) })
    await waitForLock(schema.admin, worker.pid)
    await enqueue(schema.admin, schema.table, {
      task: "t",
      queue: "q",
      key: "k1",
      runAt: T(100),
      now: T(5),
    })
    await release()
    // The snapshot saw J1 as the head; the recheck of run_at after the lock rejected it, and J2 was not claimed.
    expect(await claim).toEqual({ kind: "busy", queue: { scope: "", queue: "q" } })
  })

  test("a head that a producer moved after the claim snapshot to a later time that is still due starts before a job that is now earlier", async () => {
    // Accepted behavior: the order is that of the claim snapshot, and the recheck of run_at rejects a head only when it is no longer due.
    await enqueue(schema.admin, schema.table, { task: "t", queue: "q", key: "k1", now: T(1) })
    await enqueue(schema.admin, schema.table, { task: "t", queue: "q", key: "k2", now: T(2) })
    const worker = await schema.connect({ hooks: ["claim_queue"] })
    const release = await holdHook(schema.admin, "claim_queue")
    const claim = claimQueued(worker, schema.table, { ...W("a"), now: T(5) })
    await waitForLock(schema.admin, worker.pid)
    await enqueue(schema.admin, schema.table, {
      task: "t",
      queue: "q",
      key: "k1",
      runAt: T(3),
      now: T(5),
    })
    await release()
    const r = await claim
    expect(r.kind === "job" && [r.claim.job.key, secondsOf(r.claim.job.run_at)]).toEqual(["k1", 3])
    expect((await jobs(schema, "key = 'k2'")).map(row => row.state)).toEqual(["pending"])
  })

  test("a job that a producer inserted before the head after the claim snapshot starts after the head", async () => {
    // Accepted behavior: the order is that of the claim snapshot.
    await enqueue(schema.admin, schema.table, { task: "t", queue: "q", key: "k1", now: T(2) })
    const worker = await schema.connect({ hooks: ["claim_queue"] })
    const release = await holdHook(schema.admin, "claim_queue")
    const claim = claimQueued(worker, schema.table, { ...W("a"), now: T(5) })
    await waitForLock(schema.admin, worker.pid)
    await enqueue(schema.admin, schema.table, {
      task: "t",
      queue: "q",
      key: "k0",
      runAt: T(1),
      now: T(5),
    })
    await release()
    const r = await claim
    expect(r.kind === "job" && r.claim.job.key).toBe("k1")
    expect((await jobs(schema, "key = 'k0'")).map(row => row.state)).toEqual(["pending"])
  })

  test("a head that a producer moved to another queue after the claim snapshot does not start before the head of that queue", async () => {
    await enqueue(schema.admin, schema.table, { task: "t", queue: "a", key: "k1", now: T(2) })
    await enqueue(schema.admin, schema.table, { task: "t", queue: "b", key: "k0", now: T(1) })
    const worker = await schema.connect({ hooks: ["claim_queue"] })
    const release = await holdHook(schema.admin, "claim_queue")
    const claim = claimQueued(worker, schema.table, { ...W("a"), now: T(5) })
    await waitForLock(schema.admin, worker.pid)
    await enqueue(schema.admin, schema.table, {
      task: "t",
      queue: "b",
      key: "k1",
      runAt: T(2),
      now: T(5),
    })
    await release()
    expect(await claim).toEqual({ kind: "busy", queue: { scope: "", queue: "a" } })
    const next = await claimQueued(schema.admin, schema.table, { ...W("a"), now: T(5) })
    expect(next.kind === "job" && next.claim.job.key).toBe("k0")
  })

  test("a stale snapshot claims J3 while J2 became the head and active at another worker: 23505, and the retry claims nothing", async () => {
    await enqueue(schema.admin, schema.table, {
      task: "t",
      queue: "q",
      key: "k2",
      runAt: T(100),
      now: T(0),
    })
    const j3 = await enqueue(schema.admin, schema.table, {
      task: "t",
      queue: "q",
      key: "k3",
      runAt: T(3),
      now: T(0),
    })
    const b = await schema.connect({ hooks: ["claim_queue"] })
    const c = await schema.connect()
    const release = await holdHook(schema.admin, "claim_queue")
    const stale = claimQueued(b, schema.table, { ...W("b"), now: T(5) })
    await waitForLock(schema.admin, b.pid)
    // A producer moves J2 before J3, and another worker claims J2 as the head.
    await enqueue(schema.admin, schema.table, {
      task: "t",
      queue: "q",
      key: "k2",
      runAt: T(1),
      now: T(5),
    })
    const rc = await claimQueued(c, schema.table, { ...W("c"), now: T(5) })
    expect(rc.kind === "job" && rc.claim.job.key).toBe("k2")
    await release()
    const err = await stale.catch(e => e)
    expect(isUniqueViolation(err, "job_active_queue_idx")).toBe(true)
    const stats: ClaimCycleStats = { uniqueViolations: 0, busy: 0 }
    expect(await claimCycle(b, schema.table, { ...W("b"), slots: 1, now: T(5) }, stats)).toEqual([])
    expect((await jobs(schema, `id = ${j3.id}`))[0]!.state).toBe("pending")
  })
})

/** Serializes access to one connection from several async paths. */
function serial(conn: Conn) {
  let chain: Promise<unknown> = Promise.resolve()
  return <T>(fn: (c: Conn) => Promise<T>): Promise<T> => {
    const p = chain.then(() => fn(conn))
    chain = p.catch(() => {})
    return p
  }
}

describe("stress", () => {
  test("8 workers × 4 slots, 1000 jobs in 10 queues and 500 without a queue", async () => {
    const queued = 1000
    const plain = 500
    await query(
      schema.admin,
      `insert into ${schema.table.name} (task, queue, payload, run_at, window_started_at, created_at, max_attempts)
       select 't', 'q' || (i % 10), to_jsonb(i), now(), now(), now(), 25 from generate_series(1, $1) i`,
      [queued],
    )
    await query(
      schema.admin,
      `insert into ${schema.table.name} (task, payload, run_at, window_started_at, created_at, max_attempts)
       select 't', to_jsonb(i), now(), now(), now(), 25 from generate_series(1, $1) i`,
      [plain],
    )
    const order = new Map<string, number[]>()
    const running = new Map<string, number>()
    const completed = new Map<string, number>()
    let overlaps = 0
    let plainRunning = 0
    let plainMax = 0
    let done = 0
    const stats: ClaimCycleStats = { uniqueViolations: 0, busy: 0 }
    let cycles = 0
    let emptyCycles = 0
    // The seed fixes the sequence of delays but not which handler gets which: the claims of the workers interleave by timing.
    const random = createRandom(1)

    const handle = async (claim: Claim) => {
      const q = claim.job.queue
      if (q) {
        if ((running.get(q) ?? 0) > 0) overlaps++
        running.set(q, (running.get(q) ?? 0) + 1)
        order.set(q, [...(order.get(q) ?? []), Number(claim.id)])
      } else plainMax = Math.max(plainMax, ++plainRunning)
      await sleep(random.float() * 3)
      if (q) running.set(q, running.get(q)! - 1)
      else plainRunning--
    }

    const started = performance.now()
    await Promise.all(
      Array.from({ length: 8 }, async (_, wi) => {
        const run = serial(await schema.connect())
        const inflight = new Set<Promise<void>>()
        const claimState = createClaimState()
        while (done < queued + plain) {
          const free = 4 - inflight.size
          const claims =
            free > 0
              ? await run(c =>
                  claimCycle(
                    c,
                    schema.table,
                    { worker: `w${wi}`, tasks: ["t"], slots: free },
                    stats,
                    claimState,
                  ),
                )
              : []
          cycles++
          if (claims.length === 0) emptyCycles++
          for (const claim of claims) {
            const p = (async () => {
              await handle(claim)
              // The handler ended before complete is sent: the next claim of the queue is possible only after the commit.
              if (await run(c => complete(c, schema.table, claim))) {
                completed.set(claim.id, (completed.get(claim.id) ?? 0) + 1)
                done++
              }
            })()
            inflight.add(p)
            p.finally(() => inflight.delete(p))
          }
          if (claims.length === 0 || inflight.size >= 4) await Promise.race([...inflight, sleep(2)])
        }
      }),
    )
    const elapsed = performance.now() - started
    const fifoViolations = [...order.values()].filter(ids =>
      ids.some((id, i) => i > 0 && id < ids[i - 1]!),
    ).length
    console.log(
      `stress: ${queued + plain} jobs in ${elapsed.toFixed(0)} ms (${(((queued + plain) / elapsed) * 1000).toFixed(0)} jobs/s), ` +
        `${cycles} claim cycles (${emptyCycles} empty), 23505 retries ${stats.uniqueViolations}, busy heads ${stats.busy}, ` +
        `max plain concurrency ${plainMax}`,
    )
    expect(overlaps).toBe(0)
    expect(fifoViolations).toBe(0)
    expect(completed.size).toBe(queued + plain)
    expect([...completed.values()].every(n => n === 1)).toBe(true)
    expect(plainMax).toBeGreaterThan(4)
    expect(await jobs(schema)).toEqual([])
  }, 60_000)
})

const queueName = (i: number) => `q${String(i).padStart(2, "0")}`

/**
 * Runs `iterations` steps of one worker on a virtual clock: each step adds new work, claims up to `slots`, and completes everything it claimed.
 * Returns claims per class and per queue, and the largest wait per queue.
 */
async function simulate(o: {
  iterations: number
  slots: number
  inflow: (step: number) => { plain: number; queues: number[] }
}) {
  const state = createClaimState()
  const perQueue = new Map<string, number>()
  const maxWait = new Map<string, number>()
  let plain = 0
  let queued = 0
  for (let step = 0; step < o.iterations; step++) {
    const now = T(step)
    const work = o.inflow(step)
    for (let i = 0; i < work.plain; i++)
      await enqueue(schema.admin, schema.table, { task: "t", now })
    for (const q of work.queues)
      await enqueue(schema.admin, schema.table, { task: "t", queue: queueName(q), now })
    const claims = await claimCycle(
      schema.admin,
      schema.table,
      { worker: "w", tasks: ["t"], slots: o.slots, now },
      undefined,
      state,
    )
    for (const c of claims) {
      if (c.job.queue) {
        queued++
        perQueue.set(c.job.queue, (perQueue.get(c.job.queue) ?? 0) + 1)
        const wait = step - (c.job.run_at.getTime() - T(0).getTime()) / 1000
        maxWait.set(c.job.queue, Math.max(maxWait.get(c.job.queue) ?? 0, wait))
      } else plain++
      await complete(schema.admin, schema.table, c)
    }
  }
  return { plain, queued, perQueue, maxWait }
}

describe("claim cycle fairness under continuous inflow", () => {
  // The claim statements on a fresh table without statistics take seconds over the simulation.
  const timeout = 20_000

  test(
    "a permanent plain backlog does not starve queued jobs",
    async () => {
      // 3 plain jobs per step against 2 slots: the plain backlog only grows.
      const r = await simulate({
        iterations: 100,
        slots: 2,
        inflow: step => ({ plain: 3, queues: step % 2 ? [step % 4] : [] }),
      })
      expect(r.queued).toBeGreaterThanOrEqual(50)
      expect(r.plain).toBeGreaterThanOrEqual(80)
    },
    timeout,
  )

  test(
    "a permanent queued backlog does not starve plain jobs",
    async () => {
      const r = await simulate({
        iterations: 100,
        slots: 1,
        inflow: step => ({ plain: step % 2, queues: [0, 1, 2, 3] }),
      })
      expect(r.plain).toBeGreaterThanOrEqual(45)
    },
    timeout,
  )

  test(
    "early queues with a permanent backlog do not starve late queues",
    async () => {
      // 20 queues, every queue receives a job every step; 2 slots serve one job per queue every 10 steps at best.
      const r = await simulate({
        iterations: 200,
        slots: 2,
        inflow: () => ({ plain: 0, queues: Array.from({ length: 20 }, (_, i) => i) }),
      })
      const counts = Array.from({ length: 20 }, (_, i) => r.perQueue.get(queueName(i)) ?? 0)
      expect(Math.min(...counts)).toBeGreaterThanOrEqual(15)
      expect(Math.max(...counts) - Math.min(...counts)).toBeLessThanOrEqual(2)
    },
    timeout,
  )
})

describe("queue references", () => {
  test("U+001F in a queue name keeps the round-robin order", async () => {
    for (const queue of ["a\x1fb", "c"])
      for (let i = 0; i < 2; i++)
        await enqueue(schema.admin, schema.table, { task: "t", queue, now: T(i) })
    const state = createClaimState()
    const order: (string | null)[] = []
    for (let i = 0; i < 4; i++) {
      const claim = only(
        await claimCycle(
          schema.admin,
          schema.table,
          { ...W("w"), slots: 1, now: T(5) },
          undefined,
          state,
        ),
      )
      order.push(claim.job.queue)
      await complete(schema.admin, schema.table, claim)
    }
    expect(order).toEqual(["a\x1fb", "c", "a\x1fb", "c"])
  })
})

describe("an interrupted claim cycle", () => {
  test("a connection lost in the queue phase keeps the claims of the plain phase", async () => {
    const plain = await enqueue(schema.admin, schema.table, { task: "t", now: T(0) })
    await enqueue(schema.admin, schema.table, { task: "t", queue: "q", now: T(0) })
    const worker = await schema.connect({ hooks: ["claim_queue"] })
    const release = await holdHook(schema.admin, "claim_queue")
    const received: Claim[] = []
    const cycle = claimCycle(worker, schema.table, {
      ...W("w"),
      slots: 2,
      now: T(1),
      onClaim: claim => received.push(claim),
    })
    await waitForLock(schema.admin, worker.pid)
    await query(schema.admin, `select pg_terminate_backend($1::text::int)`, [worker.pid])
    await expect(cycle).rejects.toBeInstanceOf(ConnectionError)
    await release()
    expect(received.map(c => c.id)).toEqual([plain.id])
  })

  test("a cycle whose proceed check fails sends no further claim statement and keeps its claims", async () => {
    const plain = await enqueue(schema.admin, schema.table, { task: "t", now: T(0) })
    await enqueue(schema.admin, schema.table, { task: "t", queue: "q", now: T(0) })
    let statements = 0
    const counting: StatementTarget = {
      execute: statement => {
        statements++
        return schema.admin.execute(statement)
      },
    }
    const claims = await claimCycle(counting, schema.table, {
      ...W("w"),
      slots: 2,
      now: T(1),
      proceed: () => statements < 1,
    })
    expect([claims.map(c => c.id), statements]).toEqual([[plain.id], 1])
  })
})

const PLANS = [
  [],
  ["enable_nestloop = off"],
  ["enable_hashjoin = off", "enable_mergejoin = off"],
  ["enable_indexscan = off", "enable_indexonlyscan = off"],
  ["enable_indexscan = off", "enable_indexonlyscan = off", "enable_bitmapscan = off"],
  ["enable_seqscan = off"],
  ["enable_material = off", "enable_memoize = off"],
]

describe("advisory locks of claimQueued", () => {
  for (const busy of [0, 5, 50]) {
    for (const plan of PLANS) {
      test(`${busy} busy of 60 queues, plan [${plan.join(", ") || "default"}]: one lock held`, async () => {
        for (let i = 0; i < 60; i++)
          await enqueue(schema.admin, schema.table, { task: "t", queue: queueName(i), now: T(0) })
        await query(
          schema.admin,
          `update ${schema.table.name} set state = 'active', claim_id = id, lease_until = $1 where queue < $2`,
          [T(3600), queueName(busy)],
        )
        for (let i = 0; i < busy; i++)
          await enqueue(schema.admin, schema.table, { task: "t", queue: queueName(i), now: T(0) })
        await query(schema.admin, `analyze ${schema.table.name}`)
        const c = await schema.connect()
        for (const setting of plan) await query(c, `set ${setting}`)
        // An explicit transaction keeps xact locks until commit, which makes extra locks observable.
        await query(c, "begin")
        const r = await claimQueued(c, schema.table, { worker: "w", tasks: ["t"], now: T(1) })
        const rows = await query(
          schema.admin,
          `select count(*)::int as n from pg_locks where pid = $1::text::int and locktype = 'advisory'`,
          [c.pid],
        )
        await query(c, "rollback")
        expect(r.kind).toBe("job")
        expect(rows[0]!.n).toBe("1")
      })
    }
  }
})
