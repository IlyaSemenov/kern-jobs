// Leases: the reaper with an existing pending job and with batches, the return of the jobs of a worker, the leases that a heartbeat sets, fencing, heartbeat and complete against the reaper and a fail transaction, reaper progress under a hot key, and ownership after the lease expired but before the reaper returned the job.

import { afterEach, beforeEach, describe, expect, test } from "bun:test"

import { param, sql } from "kern-orm/db"
import { getBackendPid, waitForLock, waitForLockedStatement } from "kern-orm/testing"

import { enqueue, enqueueItems } from "../../sql/enqueue"
import { toStatementLabel } from "../../sql/statement"
import {
  createTestSchema,
  isPending,
  jobs,
  only,
  pauseAt,
  recordLabels,
  secondsOf,
  T,
  type TestSchema,
} from "../../test-harness/database"

import { claimCycle, claimPlain } from "./claim"
import { schema } from "./db"
import { heartbeat, reap } from "./lease"
import { complete, fail } from "./settlement"
import * as t from "./types"
import { isUniqueViolation } from "./violation"

let testSchema: TestSchema
beforeEach(async () => {
  testSchema = await createTestSchema()
})
afterEach(async () => {
  await testSchema.dispose()
})

const W = (worker = "dead") => ({ worker, tasks: ["t"], defaultLease: 10 })

/** A is active under a dead worker with an expired lease at T(11); B is pending with the same key. */
async function expiredWithPending(o: { maxAttempts?: number; withPending?: boolean } = {}) {
  await enqueue(testSchema.conn, testSchema.table, {
    task: "t",
    key: "k",
    payload: "a",
    maxAttempts: o.maxAttempts ?? 5,
    now: T(0),
  })
  const a = only(await claimPlain(testSchema.conn.query, { ...W(), limit: 1, now: T(0) }))
  const b =
    o.withPending === false
      ? null
      : await enqueue(testSchema.conn, testSchema.table, {
          task: "t",
          key: "k",
          payload: "b",
          runAt: T(20),
          now: T(1),
        })
  return { a, b }
}

describe("reaper with an existing pending job", () => {
  test("A below max attempts → A pending without key, B unchanged, no active rows", async () => {
    const { a, b } = await expiredWithPending()
    expect(await reap(testSchema.conn.query, { now: T(11) })).toEqual([
      { id: a.id, state: "pending", detached: true },
    ])
    const rows = await jobs(testSchema.conn.query)
    expect(rows.map(r => [r.id, r.state, r.key, r.attempts])).toEqual([
      [a.id, "pending", null, 1],
      [b!.id, "pending", "k", 0],
    ])
    expect(secondsOf(rows[1]!.runAt)).toBe(20)
  })

  test("A at max attempts → A failed with key at the time of the reaper, B unchanged", async () => {
    const { a, b } = await expiredWithPending({ maxAttempts: 1 })
    expect(await reap(testSchema.conn.query, { now: T(11) })).toEqual([
      { id: a.id, state: "failed", detached: false },
    ])
    expect(
      (await jobs(testSchema.conn.query)).map(r => [r.id, r.state, r.key, r.failedAt]),
    ).toEqual([
      [a.id, "failed", "k", T(11)],
      [b!.id, "pending", "k", null],
    ])
  })

  test("no B → A pending with its key", async () => {
    const { a } = await expiredWithPending({ withPending: false })
    expect(await reap(testSchema.conn.query, { now: T(11) })).toEqual([
      { id: a.id, state: "pending", detached: false },
    ])
  })

  test("expired jobs of one key in two scopes and a pending job in one of them: each scope keeps its own key", async () => {
    const claim = async (scope: string, now: Date) => {
      await enqueue(testSchema.conn, testSchema.table, { task: "t", scope, key: "k", now })
      return only(await claimPlain(testSchema.conn.query, { ...W(), limit: 1, now }))
    }
    const a = await claim("a", T(0))
    const c = await claim("b", T(1))
    const b = await enqueue(testSchema.conn, testSchema.table, {
      task: "t",
      scope: "b",
      key: "k",
      now: T(2),
    })
    const reaped = await reap(testSchema.conn.query, { now: T(20) })
    expect(reaped.sort((x, y) => Number(x.id) - Number(y.id))).toEqual([
      { id: a.id, state: "pending", detached: false },
      { id: c.id, state: "pending", detached: true },
    ])
    expect((await jobs(testSchema.conn.query)).map(r => [r.id, r.scope, r.state, r.key])).toEqual([
      [a.id, "a", "pending", "k"],
      [c.id, "b", "pending", null],
      [b.id, "b", "pending", "k"],
    ])
  })

  test("lease not yet expired → nothing reaped", async () => {
    await expiredWithPending()
    expect(await reap(testSchema.conn.query, { now: T(9) })).toEqual([])
  })

  test("uncommitted insert of B blocks the reaper; after commit 23505 and the retry detaches A", async () => {
    const { a } = await expiredWithPending({ withPending: false })
    const producer = await testSchema.connect()
    const reaper = await testSchema.connect()
    await producer.query`begin`
    await enqueue(producer, testSchema.table, { task: "t", key: "k", now: T(5) })
    const stats = { uniqueViolations: 0 }
    const reaped = reap(reaper.query, { now: T(11) }, stats)
    await waitForLock(testSchema.conn, reaper.pid)
    await producer.query`commit`
    expect(await reaped).toEqual([{ id: a.id, state: "pending", detached: true }])
    expect(stats.uniqueViolations).toBe(1)
  })

  test("B committed after the reaper snapshot → 23505 and the retry detaches A", async () => {
    const { a } = await expiredWithPending({ withPending: false })
    const reaper = await testSchema.connect({ pauses: ["reap"] })
    const resume = await pauseAt(testSchema.conn, "reap")
    const stats = { uniqueViolations: 0 }
    const reaped = reap(reaper.query, { now: T(11) }, stats)
    await waitForLock(testSchema.conn, reaper.pid)
    await enqueue(testSchema.conn, testSchema.table, { task: "t", key: "k", now: T(5) })
    await resume()
    expect(await reaped).toEqual([{ id: a.id, state: "pending", detached: true }])
    expect(stats.uniqueViolations).toBe(1)
  })

  test("uncommitted insert of key A: the reaper waits a bounded time, returns independent B, and leaves A for the next round", async () => {
    const a = await enqueue(testSchema.conn, testSchema.table, {
      task: "t",
      key: "blocked",
      now: T(0),
    })
    const b = await enqueue(testSchema.conn, testSchema.table, {
      task: "t",
      key: "independent",
      now: T(0),
    })
    await claimPlain(testSchema.conn.query, { ...W(), limit: 2, now: T(0) })
    const producer = await testSchema.connect()
    await producer.query`begin`
    await enqueue(producer, testSchema.table, { task: "t", key: "blocked", now: T(5) })

    const reaper = await testSchema.connect()
    const stats = { uniqueViolations: 0, lockTimeouts: 0, forcedDetaches: 0 }
    const reaped = await reap(reaper.query, { now: T(11), lockTimeout: 0.2 }, stats)
    expect(reaped).toEqual([{ id: b.id, state: "pending", detached: false }])
    // The batch timed out, and so did the row of A on its own.
    expect([stats.lockTimeouts, stats.uniqueViolations, stats.forcedDetaches]).toEqual([2, 0, 0])
    expect((await jobs(testSchema.conn.query)).map(r => [r.id, r.state, r.key])).toEqual([
      [a.id, "active", "blocked"],
      [b.id, "pending", "independent"],
      // The producer's row is not visible to the test connection until it commits.
    ])

    // The timeout left no setting on the connection, and the reaper works after the conflict.
    expect(await reaper.query`show lock_timeout`.get(t.text)).toBe("0")
    await producer.query`rollback`
    expect(await reap(reaper.query, { now: T(11), lockTimeout: 0.2 })).toEqual([
      { id: a.id, state: "pending", detached: false },
    ])
  })

  test("uncommitted insert of key A that commits after the timeout: the next round detaches A", async () => {
    await enqueue(testSchema.conn, testSchema.table, { task: "t", key: "blocked", now: T(0) })
    const a = only(await claimPlain(testSchema.conn.query, { ...W(), limit: 1, now: T(0) }))
    const producer = await testSchema.connect()
    await producer.query`begin`
    await enqueue(producer, testSchema.table, { task: "t", key: "blocked", now: T(5) })
    const reaper = await testSchema.connect()
    expect(await reap(reaper.query, { now: T(11), lockTimeout: 0.2 })).toEqual([])
    await producer.query`commit`
    expect(await reap(reaper.query, { now: T(11), lockTimeout: 0.2 })).toEqual([
      { id: a.id, state: "pending", detached: true },
    ])
  })

  test("forcedDetaches counts the rows that lost their key, not the forced attempts", async () => {
    const a = await enqueue(testSchema.conn, testSchema.table, {
      task: "t",
      key: "blocked",
      now: T(0),
    })
    const c = await enqueue(testSchema.conn, testSchema.table, { task: "t", now: T(0) })
    await claimPlain(testSchema.conn.query, { ...W(), limit: 2, now: T(0) })
    const producer = await testSchema.connect()
    await producer.query`begin`
    await enqueue(producer, testSchema.table, { task: "t", key: "blocked", now: T(5) })
    const stats = { uniqueViolations: 0, lockTimeouts: 0, forcedDetaches: 0 }
    // Every row of the fallback is a forced attempt; only the keyed one loses a key, and it does not wait for the producer.
    const reaped = await reap(
      testSchema.conn.query,
      { now: T(11), lockTimeout: 0.2, maxRetries: 0 },
      stats,
    )
    await producer.query`rollback`
    expect(reaped).toEqual([
      { id: a.id, state: "pending", detached: true },
      { id: c.id, state: "pending", detached: false },
    ])
    expect([stats.lockTimeouts, stats.forcedDetaches]).toEqual([1, 1])
  })

  test("two expired active rows with one key: newest keeps the key, older is detached, no 23505", async () => {
    // An event during active creates B; B is claimed too; the worker holding both dies.
    await enqueue(testSchema.conn, testSchema.table, {
      task: "t",
      key: "k",
      payload: "a",
      now: T(0),
    })
    const a = only(await claimPlain(testSchema.conn.query, { ...W(), limit: 1, now: T(0) }))
    await enqueue(testSchema.conn, testSchema.table, {
      task: "t",
      key: "k",
      payload: "b",
      now: T(1),
    })
    const b = only(await claimPlain(testSchema.conn.query, { ...W(), limit: 1, now: T(1) }))
    const stats = { uniqueViolations: 0 }
    const reaped = await reap(testSchema.conn.query, { now: T(20) }, stats)
    expect(reaped.sort((x, y) => Number(x.id) - Number(y.id))).toEqual([
      { id: a.id, state: "pending", detached: true },
      { id: b.id, state: "pending", detached: false },
    ])
    expect(stats.uniqueViolations).toBe(0)
  })

  test("a reaper statement that does not rank the rows of a key fails on the same case on every retry", async () => {
    await enqueue(testSchema.conn, testSchema.table, { task: "t", key: "k", now: T(0) })
    await claimPlain(testSchema.conn.query, { ...W(), limit: 1, now: T(0) })
    await enqueue(testSchema.conn, testSchema.table, { task: "t", key: "k", now: T(1) })
    await claimPlain(testSchema.conn.query, { ...W(), limit: 1, now: T(1) })
    const now = param(t.timestamptz, T(20))
    const reapUnranked = () => testSchema.conn.query`
      update ${schema.job} j set
        state = case when j.attempts >= j.max_attempts then 'failed' else 'pending' end,
        key = case
          when j.attempts >= j.max_attempts then j.key
          when exists (select 1 from ${schema.job} p where p.state = 'pending' and p.scope = j.scope and p.task = j.task and p.key = j.key) then null
          else j.key end,
        run_at = ${now}, not_before = ${now}, window_started_at = ${now}, claim_id = null, locked_by = null, lease_until = null
      where j.state = 'active' and j.lease_until < ${now}`
    for (let i = 0; i < 3; i++) {
      const err = await reapUnranked().catch(e => e)
      expect(isUniqueViolation(err, "job_open_key_idx")).toBe(true)
    }
  })

  test("reaping a queued job releases the queue for the next job", async () => {
    await enqueue(testSchema.conn, testSchema.table, {
      task: "t",
      queue: "q",
      payload: 1,
      now: T(0),
    })
    await enqueue(testSchema.conn, testSchema.table, {
      task: "t",
      queue: "q",
      payload: 2,
      now: T(0),
    })
    await claimCycle(testSchema.conn.query, { ...W(), slots: 1, now: T(0) })
    expect(await claimCycle(testSchema.conn.query, { ...W("live"), slots: 1, now: T(5) })).toEqual(
      [],
    )
    await reap(testSchema.conn.query, { now: T(11) })
    // The reaped job gets run_at = now and goes behind the job that was already waiting.
    const next = only(
      await claimCycle(testSchema.conn.query, { ...W("live"), slots: 1, now: T(11) }),
    )
    expect(next.job.payload).toBe(2)
  })
})

describe("reaper with batches", () => {
  /** Appends a batch of task `t` and the empty key at `now`. */
  const append = (payload: number[], now: Date, maxAttempts = 5) =>
    enqueueItems(testSchema.conn, testSchema.table, {
      task: "t",
      key: "",
      maxItems: 100,
      payload,
      maxAttempts,
      now,
    })

  test("returns an expired batch to pending sealed with its id, payload, and key, beside its successor", async () => {
    await append([1], T(0))
    const a = only(await claimPlain(testSchema.conn.query, { ...W(), limit: 1, now: T(0) }))
    const [b] = await append([2, 3], T(1))
    const stats = { uniqueViolations: 0 }
    expect(await reap(testSchema.conn.query, { now: T(11) }, stats)).toEqual([
      { id: a.id, state: "pending", detached: false },
    ])
    expect(stats.uniqueViolations).toBe(0)
    expect(
      (await jobs(testSchema.conn.query)).map(r => [
        r.id,
        r.state,
        r.key,
        r.payload,
        r.sealed,
        r.attempts,
      ]),
    ).toEqual([
      [a.id, "pending", "", [1], true, 1],
      [b!.id, "pending", "", [2, 3], false, 0],
    ])
  })

  test("returns two expired batches of one key without detaching either", async () => {
    await append([1], T(0))
    const a = only(await claimPlain(testSchema.conn.query, { ...W(), limit: 1, now: T(0) }))
    await append([2], T(1))
    const b = only(await claimPlain(testSchema.conn.query, { ...W(), limit: 1, now: T(1) }))
    await append([3], T(2))
    const reaped = await reap(testSchema.conn.query, { now: T(20) })
    expect(reaped.sort((x, y) => Number(x.id) - Number(y.id))).toEqual([
      { id: a.id, state: "pending", detached: false },
      { id: b.id, state: "pending", detached: false },
    ])
    expect((await jobs(testSchema.conn.query)).map(r => [r.payload, r.sealed])).toEqual([
      [[1], true],
      [[2], true],
      [[3], false],
    ])
  })

  test("fails an expired batch on its last attempt with its payload", async () => {
    await append([1], T(0), 1)
    const a = only(await claimPlain(testSchema.conn.query, { ...W(), limit: 1, now: T(0) }))
    await append([2], T(1))
    expect(await reap(testSchema.conn.query, { now: T(11) })).toEqual([
      { id: a.id, state: "failed", detached: false },
    ])
    expect((await jobs(testSchema.conn.query)).map(r => [r.state, r.payload])).toEqual([
      ["failed", [1]],
      ["pending", [2]],
    ])
  })
})

describe("returning the active jobs of a worker", () => {
  test("returns the active jobs of the worker before their leases expire, and leaves those of another worker", async () => {
    const { a } = await expiredWithPending({ withPending: false })
    await enqueue(testSchema.conn, testSchema.table, { task: "t", now: T(0) })
    const other = only(
      await claimPlain(testSchema.conn.query, { ...W("live"), limit: 1, now: T(0) }),
    )
    expect(await reap(testSchema.conn.query, { worker: "dead", now: T(1) })).toEqual([
      { id: a.id, state: "pending", detached: false },
    ])
    const rows = await jobs(testSchema.conn.query)
    expect(rows.map(r => [r.id, r.state, r.key, r.attempts, r.lockedBy, r.lastError])).toEqual([
      [a.id, "pending", "k", 1, null, "worker restarted"],
      [other.id, "active", null, 1, "live", null],
    ])
    expect(secondsOf(rows[0]!.runAt)).toBe(1)
  })

  test("a job of the worker on its last attempt fails", async () => {
    const { a } = await expiredWithPending({ maxAttempts: 1, withPending: false })
    expect(await reap(testSchema.conn.query, { worker: "dead", now: T(1) })).toEqual([
      { id: a.id, state: "failed", detached: false },
    ])
  })

  test("B committed after the snapshot → 23505, and the retry row by row detaches the job of the worker", async () => {
    const { a } = await expiredWithPending({ withPending: false })
    const reaper = await testSchema.connect({ pauses: ["reap"] })
    const resume = await pauseAt(testSchema.conn, "reap")
    const stats = { uniqueViolations: 0 }
    const reaped = reap(reaper.query, { worker: "dead", now: T(1) }, stats)
    await waitForLock(testSchema.conn, reaper.pid)
    await enqueue(testSchema.conn, testSchema.table, { task: "t", key: "k", now: T(1) })
    await resume()
    expect(await reaped).toEqual([{ id: a.id, state: "pending", detached: true }])
    expect(stats.uniqueViolations).toBe(1)
  })
})

describe("heartbeat", () => {
  test("never shortens the lease", async () => {
    await enqueue(testSchema.conn, testSchema.table, { task: "t", now: T(0) })
    const a = only(
      await claimPlain(testSchema.conn.query, {
        ...W(),
        defaultLease: 100,
        limit: 1,
        now: T(0),
      }),
    )
    await heartbeat(testSchema.conn.query, { claims: [{ ...a, lease: 10 }], now: T(1) })
    expect(secondsOf((await jobs(testSchema.conn.query))[0]!.leaseUntil!)).toBe(100)
  })

  test("each claim gets its own lease", async () => {
    await enqueue(testSchema.conn, testSchema.table, { task: "t", now: T(0) })
    await enqueue(testSchema.conn, testSchema.table, { task: "t", now: T(0) })
    const [a, b] = await claimPlain(testSchema.conn.query, { ...W(), limit: 2, now: T(0) })
    await heartbeat(testSchema.conn.query, {
      claims: [
        { ...a!, lease: 30 },
        { ...b!, lease: 300 },
      ],
      now: T(5),
    })
    const leaseOf = async (id: string) =>
      secondsOf(only(await jobs(testSchema.conn.query, sql`id = ${id}`)).leaseUntil!)
    expect([await leaseOf(a!.id), await leaseOf(b!.id)]).toEqual([35, 305])
  })

  test("a claim and a heartbeat take the longest lease, 1e12 seconds", async () => {
    const longest = 1e12
    await enqueue(testSchema.conn, testSchema.table, { task: "t", now: T(0) })
    const a = only(
      await claimPlain(testSchema.conn.query, {
        ...W(),
        defaultLease: longest,
        limit: 1,
        now: T(0),
      }),
    )
    expect(secondsOf(only(await jobs(testSchema.conn.query)).leaseUntil!)).toBe(longest)
    await heartbeat(testSchema.conn.query, { claims: [{ ...a, lease: longest }], now: T(5) })
    expect(secondsOf((await jobs(testSchema.conn.query))[0]!.leaseUntil!)).toBe(longest + 5)
  })

  test("labels its statement", async () => {
    const { target, labels } = recordLabels(testSchema.conn)
    await heartbeat(testSchema.db.$queryOn(target), {
      claims: [{ id: "1", claimId: "1", lease: 10 }],
      now: T(0),
    })
    expect(labels).toEqual([toStatementLabel("heartbeat:extend")])
  })
})

describe("fencing", () => {
  test("after reaping, the old claim cannot heartbeat or complete", async () => {
    const { a } = await expiredWithPending({ withPending: false })
    await reap(testSchema.conn.query, { now: T(11) })
    const again = only(
      await claimPlain(testSchema.conn.query, { ...W("live"), limit: 1, now: T(11) }),
    )
    expect(
      await heartbeat(testSchema.conn.query, { claims: [{ ...a, lease: 10 }], now: T(12) }),
    ).toEqual({
      confirmed: [],
      busy: [],
    })
    expect(await complete(testSchema.db, a)).toBe(false)
    expect(await complete(testSchema.db, again)).toBe(true)
  })

  test("ABA: same worker reclaims after reap and resetAttempts; (id, worker, attempts) would match, claim_id does not", async () => {
    await enqueue(testSchema.conn, testSchema.table, { task: "t", key: "k", now: T(0) })
    const first = only(await claimPlain(testSchema.conn.query, { ...W("w1"), limit: 1, now: T(0) }))
    await reap(testSchema.conn.query, { now: T(11) })
    await enqueue(testSchema.conn, testSchema.table, {
      task: "t",
      key: "k",
      resetAttempts: true,
      now: T(11),
    })
    const second = only(
      await claimPlain(testSchema.conn.query, { ...W("w1"), limit: 1, now: T(11) }),
    )
    expect([
      second.id,
      only(await jobs(testSchema.conn.query)).lockedBy,
      second.job.attempts,
    ]).toEqual([first.id, "w1", first.job.attempts])
    expect(await complete(testSchema.db, first)).toBe(false)
    expect(await complete(testSchema.db, second)).toBe(true)
  })

  test("heartbeat in flight: reaper skips the locked row, lease is extended", async () => {
    const { a } = await expiredWithPending({ withPending: false })
    const worker = await testSchema.connect()
    await worker.query`begin`
    expect(await heartbeat(worker.query, { claims: [{ ...a, lease: 10 }], now: T(10.5) })).toEqual({
      confirmed: [a.claimId],
      busy: [],
    })
    expect(await reap(testSchema.conn.query, { now: T(11) })).toEqual([])
    await worker.query`commit`
    expect(await reap(testSchema.conn.query, { now: T(11) })).toEqual([])
    expect(secondsOf((await jobs(testSchema.conn.query))[0]!.leaseUntil!)).toBe(20.5)
  })

  test("reaper in flight: heartbeat reports the claim busy without waiting, complete waits; then both see it lost", async () => {
    const { a } = await expiredWithPending({ withPending: false })
    const reaper = await testSchema.connect()
    const worker = testSchema.createDb({ max: 1 })
    const workerPid = await getBackendPid(worker.$database)
    await reaper.query`begin`
    await reap(reaper.query, { now: T(11) })
    const claims = [{ ...a, lease: 10 }]
    expect(await heartbeat(testSchema.conn.query, { claims, now: T(11) })).toEqual({
      confirmed: [],
      busy: [a.claimId],
    })
    const done = complete(worker, a)
    await waitForLock(testSchema.conn, workerPid)
    await reaper.query`commit`
    expect(await done).toBe(false)
    expect(await heartbeat(testSchema.conn.query, { claims, now: T(11) })).toEqual({
      confirmed: [],
      busy: [],
    })
    expect((await jobs(testSchema.conn.query))[0]!.state).toBe("pending")
  })

  test("a fail transaction waiting for a producer: the heartbeat reports its claim busy and extends the others without waiting", async () => {
    const keyed = await enqueue(testSchema.conn, testSchema.table, {
      task: "t",
      key: "k",
      now: T(0),
    })
    await enqueue(testSchema.conn, testSchema.table, { task: "t", now: T(0) })
    const claims = await claimPlain(testSchema.conn.query, { ...W("w"), limit: 2, now: T(0) })
    const a = claims.find(x => x.id === keyed.id)
    const c = claims.find(x => x.id !== keyed.id)
    await enqueue(testSchema.conn, testSchema.table, {
      task: "t",
      key: "k",
      runAt: T(20),
      now: T(1),
    })
    const producer = await testSchema.connect()
    await producer.query`begin`
    await enqueue(producer, testSchema.table, { task: "t", key: "k", runAt: T(20), now: T(1) })
    const failing = fail(testSchema.db, a!, {
      error: "boom",
      backoff: 30,
      now: T(2),
    })
    await waitForLockedStatement(testSchema.conn, toStatementLabel("settle:lock-competitor"))
    const beat = heartbeat(testSchema.conn.query, {
      claims: [a!, c!].map(x => ({ ...x, lease: 10 })),
      now: T(3),
    })
    expect(await isPending(beat, 500)).toBe(false)
    expect(await beat).toEqual({ confirmed: [c!.claimId], busy: [a!.claimId] })
    await producer.query`commit`
    expect(await failing).toBe("superseded")
    expect(secondsOf((await jobs(testSchema.conn.query, sql`id = ${c!.id}`))[0]!.leaseUntil!)).toBe(
      13,
    )
  })
})

describe("reaper progress", () => {
  test("a hot key that conflicts on every attempt does not block 50 independent expired jobs", async () => {
    for (let i = 0; i < 50; i++)
      await enqueue(testSchema.conn, testSchema.table, { task: "t", key: `k${i}`, now: T(0) })
    await enqueue(testSchema.conn, testSchema.table, { task: "hot", key: "h", now: T(0) })
    await claimPlain(testSchema.conn.query, {
      worker: "dead",
      tasks: ["t", "hot"],
      limit: 100,
      defaultLease: 10,
      now: T(0),
    })

    const reaper = await testSchema.connect({ pauses: ["reap"] })
    const producer = await testSchema.connect()
    let resume = await pauseAt(testSchema.conn, "reap")
    const stats = { uniqueViolations: 0, forcedDetaches: 0 }
    let settled = false
    // Hot key pressure: before every attempt a live worker claims the pending "h", so the snapshot sees none;
    // inside the statement, after the snapshot, a new pending "h" is committed, so the write conflicts.
    const reaped = reap(
      reaper.query,
      {
        now: T(11),
        maxRetries: 3,
        pause: {
          beforeAttempt: () =>
            claimPlain(producer.query, {
              worker: "live",
              tasks: ["hot"],
              limit: 10,
              defaultLease: 3600,
              now: T(11),
            }).then(() => {}),
        },
      },
      stats,
    ).finally(() => {
      settled = true
    })
    reaped.catch(() => {})
    for (let attempt = 0; attempt < 200 && !settled; attempt++) {
      const blocked = await waitForLock(testSchema.conn, reaper.pid, { timeout: 1000 }).then(
        () => true,
        () => false,
      )
      if (!blocked) break
      await enqueue(producer, testSchema.table, { task: "hot", key: "h", now: T(11) })
      await resume()
      resume = await pauseAt(testSchema.conn, "reap")
    }
    await resume()
    const result = await reaped
    expect(result.length).toBe(51)
    const rows = await jobs(testSchema.conn.query, sql`task = 't'`)
    expect(rows.every(r => r.state === "pending" && r.key != null)).toBe(true)
    // The original hot row was the first "hot" job; it ends up pending without its key.
    const hot = only(
      await jobs(testSchema.conn.query, sql`task = 'hot' and attempts = 1 and claim_id is null`),
    )
    expect([hot.state, hot.key, stats.forcedDetaches]).toEqual(["pending", null, 1])
    console.log(`hot key reaper: ${stats.uniqueViolations} unique violations retried`)
  })
})

describe("ownership after lease expiry and before reclaim", () => {
  // The claim token is the authority; lease_until only makes the row eligible for the reaper.
  test("complete after lease expiry succeeds while the row has not been reaped", async () => {
    await enqueue(testSchema.conn, testSchema.table, { task: "t", now: T(0) })
    const a = only(
      await claimPlain(testSchema.conn.query, {
        worker: "w",
        tasks: ["t"],
        limit: 1,
        defaultLease: 10,
        now: T(0),
      }),
    )
    // Complete takes no virtual time, so the lease expires in server time.
    await testSchema.conn.query`
      update ${schema.job} set lease_until = now() - interval '1 second'`
    expect(await complete(testSchema.db, a)).toBe(true)
  })

  test("heartbeat after lease expiry revives the lease and the reaper skips the row", async () => {
    await enqueue(testSchema.conn, testSchema.table, { task: "t", now: T(0) })
    const a = only(
      await claimPlain(testSchema.conn.query, {
        worker: "w",
        tasks: ["t"],
        limit: 1,
        defaultLease: 10,
        now: T(0),
      }),
    )
    expect(
      await heartbeat(testSchema.conn.query, { claims: [{ ...a, lease: 10 }], now: T(15) }),
    ).toEqual({
      confirmed: [a.claimId],
      busy: [],
    })
    expect(secondsOf((await jobs(testSchema.conn.query))[0]!.leaseUntil!)).toBe(25)
    expect(await reap(testSchema.conn.query, { now: T(20) })).toEqual([])
  })
})
