// Enqueue: key modes, the uniqueness of pending keys under concurrent producers, events during `active`, the pending row held by an uncommitted enqueue, and trailing-edge debounce with maxWait on virtual time.

import { afterEach, beforeEach, describe, expect, test } from "bun:test"

import { param, sql } from "kern-orm/db"
import { waitForLock } from "kern-orm/testing"

import {
  type Conn,
  createTestSchema,
  isPending,
  jobs,
  only,
  secondsOf,
  T,
  type TestSchema,
} from "../test-harness/database"
import { createRandom } from "../test-harness/random"
import { type Claim, claimPlain } from "../worker/db/claim"
import { workerSchema } from "../worker/db/db"
import { complete, fail } from "../worker/db/settlement"
import * as t from "../worker/db/types"

import { enqueue, enqueueMany, type EnqueueParams } from "./enqueue"
import type { KeyMode } from "./job"
import type { ValuesTarget } from "./statement"

let testSchema: TestSchema
beforeEach(async () => {
  testSchema = await createTestSchema()
})
afterEach(async () => {
  await testSchema.dispose()
})

const W = { worker: "w1", tasks: ["t"] }

describe("run time", () => {
  test("is the time of the statement plus delay, which runAt overrides", async () => {
    await enqueue(testSchema.conn, testSchema.table, { task: "t", key: "now", now: T(1) })
    await enqueue(testSchema.conn, testSchema.table, {
      task: "t",
      key: "delay",
      delay: 5,
      now: T(1),
    })
    await enqueue(testSchema.conn, testSchema.table, {
      task: "t",
      key: "runAt",
      runAt: T(7),
      delay: 5,
      now: T(1),
    })
    expect((await jobs(testSchema.conn.query)).map(row => secondsOf(row.runAt))).toEqual([1, 6, 7])
  })

  test("replace takes the delay from its own statement, and keep keeps the run time", async () => {
    await enqueue(testSchema.conn, testSchema.table, { task: "t", key: "k", delay: 5, now: T(1) })
    await enqueue(testSchema.conn, testSchema.table, { task: "t", key: "k", delay: 5, now: T(3) })
    expect(secondsOf(only(await jobs(testSchema.conn.query)).runAt)).toBe(8)
    await enqueue(testSchema.conn, testSchema.table, {
      task: "t",
      key: "k",
      mode: "keep",
      delay: 1,
      now: T(4),
    })
    expect(secondsOf(only(await jobs(testSchema.conn.query)).runAt)).toBe(8)
  })

  test("takes the bounds of the options of an enqueue: delay and maxWait of 1e12 seconds, runAt in the years 1 and 9999", async () => {
    const enqueueAt = (key: string, o: Partial<EnqueueParams>) =>
      enqueue(testSchema.conn, testSchema.table, { task: "t", key, now: T(0), ...o })
    const first = new Date("0001-01-01T00:00:00.000Z")
    const last = new Date("9999-12-31T23:59:59.999Z")
    await enqueueAt("first", { runAt: first, maxAttempts: 2 ** 31 - 1 })
    await enqueueAt("last", { runAt: last })
    await enqueueAt("delay", { delay: 1e12 })
    await enqueueAt("debounce", { mode: "debounce", delay: 1e12, maxWait: 1e12 })
    // Over the pending row, the debounce adds delay to the time of its enqueue and maxWait to the start of the window.
    await enqueueAt("debounce", { mode: "debounce", delay: 1e12, maxWait: 1e12, now: T(1) })
    const rows = await jobs(testSchema.conn.query)
    expect(rows.map(row => [row.key, row.runAt.getTime()])).toEqual([
      ["first", first.getTime()],
      ["last", last.getTime()],
      ["delay", T(1e12).getTime()],
      ["debounce", T(1e12).getTime()],
    ])
    expect(rows[0]!.maxAttempts).toBe(2 ** 31 - 1)
  })
})

test("stores an absent payload as SQL NULL, read as undefined, and keeps a null payload as JSON", async () => {
  await enqueue(testSchema.conn, testSchema.table, { task: "absent" })
  await enqueue(testSchema.conn, testSchema.table, { task: "null", payload: null })
  const absent = await testSchema.conn.query`
    select payload is null from ${workerSchema.job} order by id`.pluck(t.boolean)
  expect(absent).toEqual([true, false])
  expect((await jobs(testSchema.conn.query)).map(row => row.payload)).toEqual([undefined, null])
})

describe("key modes with an existing pending job", () => {
  test("keep changes neither payload, run_at, nor max_attempts", async () => {
    const a = await enqueue(testSchema.conn, testSchema.table, {
      task: "t",
      key: "k",
      payload: 1,
      runAt: T(5),
      maxAttempts: 3,
      mode: "keep",
      now: T(0),
    })
    const b = await enqueue(testSchema.conn, testSchema.table, {
      task: "t",
      key: "k",
      payload: 2,
      runAt: T(1),
      maxAttempts: 9,
      mode: "keep",
      now: T(1),
    })
    expect(a.created).toBe(true)
    expect(b).toEqual({ id: a.id, created: false })
    const row = only(await jobs(testSchema.conn.query))
    expect(row.payload).toBe(1)
    expect(secondsOf(row.runAt)).toBe(5)
    expect(row.maxAttempts).toBe(3)
  })

  test("replace changes payload, run_at, and max_attempts and keeps attempts", async () => {
    await enqueue(testSchema.conn, testSchema.table, { task: "t", key: "k", payload: 1, now: T(0) })
    await testSchema.conn.query`update ${workerSchema.job} set attempts = 3`
    await enqueue(testSchema.conn, testSchema.table, {
      task: "t",
      key: "k",
      payload: 2,
      runAt: T(7),
      maxAttempts: 9,
      now: T(1),
    })
    const row = only(await jobs(testSchema.conn.query))
    expect(row.payload).toBe(2)
    expect(secondsOf(row.runAt)).toBe(7)
    expect(row.maxAttempts).toBe(9)
    expect(row.attempts).toBe(3)
  })

  test("replace with resetAttempts resets attempts", async () => {
    await enqueue(testSchema.conn, testSchema.table, { task: "t", key: "k", now: T(0) })
    await testSchema.conn.query`update ${workerSchema.job} set attempts = 3`
    await enqueue(testSchema.conn, testSchema.table, {
      task: "t",
      key: "k",
      resetAttempts: true,
      now: T(1),
    })
    expect((await jobs(testSchema.conn.query))[0]!.attempts).toBe(0)
  })

  test("a key is unique within (scope, task)", async () => {
    await enqueue(testSchema.conn, testSchema.table, { task: "t", key: "k", now: T(0) })
    await enqueue(testSchema.conn, testSchema.table, { task: "u", key: "k", now: T(0) })
    await enqueue(testSchema.conn, testSchema.table, {
      task: "t",
      scope: "s2",
      key: "k",
      now: T(0),
    })
    expect(await jobs(testSchema.conn.query)).toHaveLength(3)
  })
})

describe("enqueueMany", () => {
  /** Returns a target of the statements of the test schema that gives the rows of each result in reverse, an order that the statement does not exclude. */
  const createReversingTarget = (): ValuesTarget => ({
    async execute(statement) {
      const result = await testSchema.conn.execute(statement)
      return { ...result, rows: [...result.rows].reverse() }
    },
  })

  test("inserts new rows in the order of the jobs, and resolves to the result of each job in that order, a merged job to the id of the pending job of its key, whatever the order of the rows of the result", async () => {
    const existing = await enqueue(testSchema.conn, testSchema.table, {
      task: "t",
      key: "k",
      payload: 0,
    })
    const results = await enqueueMany(createReversingTarget(), testSchema.table, {
      task: "t",
      jobs: [
        { payload: "unkeyed 1" },
        { key: "k", payload: "merged" },
        { key: "b", payload: "keyed b" },
        { payload: "unkeyed 2" },
        { key: "a", payload: "keyed a" },
        { payload: "unkeyed 3" },
      ],
    })
    expect(results.map(r => r.created)).toEqual([true, false, true, true, true, true])
    expect(results[1]).toEqual({ id: existing.id, created: false })
    const rows = await jobs(testSchema.conn.query)
    const payloadOf = (id: string) => rows.find(row => row.id === id)!.payload
    expect(results.map(r => payloadOf(r.id))).toEqual([
      "unkeyed 1",
      "merged",
      "keyed b",
      "unkeyed 2",
      "keyed a",
      "unkeyed 3",
    ])
    // `jobs` reads the rows in the order of their ids.
    expect(rows.map(row => row.payload)).toEqual([
      "merged",
      "unkeyed 1",
      "keyed b",
      "unkeyed 2",
      "keyed a",
      "unkeyed 3",
    ])
  })

  test("merges each job into the pending job of its key by its own mode and settings", async () => {
    const enqueueAt = (key: string, o: Partial<EnqueueParams>) =>
      enqueue(testSchema.conn, testSchema.table, { task: "t", key, payload: 1, now: T(0), ...o })
    const debounce = { mode: "debounce", delay: 2, maxWait: 10 } as const
    await enqueueAt("keep", { runAt: T(5), maxAttempts: 3 })
    await enqueueAt("replace", {})
    await enqueueAt("__proto__", {})
    await enqueueAt("capped", debounce)
    await enqueueAt("delayed", debounce)
    await enqueueAt("held", {})
    await enqueueAt("opens", {})
    await enqueueAt("backoff", {})
    const backoffEnd = param(t.timestamptz, T(30))
    await testSchema.conn.query`
      update ${workerSchema.job} set attempts = 3,
        not_before = case when key in ('held', 'backoff') then ${backoffEnd} end,
        run_at = case when key in ('held', 'backoff') then ${backoffEnd} else run_at end`
    const results = await enqueueMany(testSchema.conn, testSchema.table, {
      task: "t",
      now: T(1),
      jobs: [
        { key: "keep", payload: 2, runAt: T(1), maxAttempts: 9, mode: "keep" },
        { key: "replace", payload: 2, runAt: T(7), maxAttempts: 9 },
        { key: "__proto__", payload: 2, resetAttempts: true },
        // Due at least(1 + 9, 0 + 6) in the window that opened at 0.
        { key: "capped", payload: 2, mode: "debounce", delay: 9, maxWait: 6 },
        // Due at least(1 + 3, 0 + 20) in the window that opened at 0.
        { key: "delayed", payload: 2, mode: "debounce", delay: 3, maxWait: 20 },
        // Opens a window at 1 over a row of another mode, due at 2, below the end of the backoff.
        { key: "held", payload: 2, mode: "debounce", delay: 1, maxWait: 10 },
        // Opens a window at 1 over a row of another mode.
        { key: "opens", payload: 2, mode: "debounce", delay: 3, maxWait: 10 },
        { key: "backoff", payload: 2 },
      ],
    })
    expect(results.every(r => !r.created)).toBe(true)
    const rows = await jobs(testSchema.conn.query)
    expect(
      rows.map(row => [
        row.key,
        row.payload,
        secondsOf(row.runAt),
        secondsOf(row.windowStartedAt),
        row.attempts,
        row.maxAttempts,
      ]),
    ).toEqual([
      ["keep", 1, 5, 0, 3, 3],
      ["replace", 2, 7, 0, 3, 9],
      ["__proto__", 2, 1, 0, 0, 25],
      ["capped", 2, 6, 0, 3, 25],
      ["delayed", 2, 4, 0, 3, 25],
      ["held", 2, 30, 1, 3, 25],
      ["opens", 2, 4, 1, 3, 25],
      ["backoff", 2, 30, 0, 3, 25],
    ])
  })

  test("stores each payload as enqueue serializes it: by its own toJSON with the empty key, absent as SQL NULL, and null as JSON", async () => {
    const results = await enqueueMany(testSchema.conn, testSchema.table, {
      task: "t",
      jobs: [
        { payload: { toJSON: (key: string) => key } },
        { payload: { toJSON: () => undefined } },
        { payload: null },
      ],
    })
    const stored = await testSchema.conn.query`
      select id, payload::text from ${workerSchema.job} order by id`.select({
      id: t.bigintText,
      payload: t.text.nullable(),
    })
    expect(results.map(r => stored.find(row => row.id === r.id)!.payload)).toEqual([
      '""',
      null,
      "null",
    ])
  })

  test("enqueues all jobs or none: a job that a check of the table rejects rolls back the others", async () => {
    await enqueue(testSchema.conn, testSchema.table, { task: "t", key: "k", payload: 1 })
    await expect(
      enqueueMany(testSchema.conn, testSchema.table, {
        task: "t",
        jobs: [{ key: "k", payload: 2 }, { payload: 3 }, { key: "a".repeat(1025), payload: 4 }],
      }),
    ).rejects.toThrow()
    expect((await jobs(testSchema.conn.query)).map(row => [row.key, row.payload])).toEqual([
      ["k", 1],
    ])
  })

  test("resolves to no results without a statement for no jobs", async () => {
    const target = { execute: () => Promise.reject(new Error("no statement")) }
    expect(await enqueueMany(target, testSchema.table, { task: "t", jobs: [] })).toEqual([])
  })

  for (const outcome of ["commit", "rollback"] as const) {
    test(`a job waits for an uncommitted insert of its key, and after a ${outcome} ${outcome === "commit" ? "merges into it" : "inserts its own row"}`, async () => {
      const p1 = await testSchema.connect()
      const p2 = await testSchema.connect()
      await p1.query`begin`
      const first = await enqueue(p1, testSchema.table, { task: "t", key: "k", payload: 1 })
      const batch = enqueueMany(p2, testSchema.table, {
        task: "t",
        jobs: [{ payload: 2 }, { key: "k", payload: 3 }],
      })
      await waitForLock(testSchema.conn, p2.pid)
      await (outcome === "commit" ? p1.query`commit` : p1.query`rollback`)
      const [unkeyed, keyed] = await batch
      expect(unkeyed!.created).toBe(true)
      expect(keyed).toEqual(
        outcome === "commit"
          ? { id: first.id, created: false }
          : { id: expect.not.stringMatching(`^${first.id}$`), created: true },
      )
      const rows = await jobs(testSchema.conn.query)
      expect(rows.find(row => row.id === keyed!.id)!.payload).toBe(3)
      expect(rows).toHaveLength(2)
    })
  }
})

describe("concurrent producers", () => {
  for (const mode of ["replace", "keep", "debounce"] as KeyMode[]) {
    test(`100 producers on 20 connections, ${mode}: one row, created by exactly one`, async () => {
      const conns = await Promise.all(Array.from({ length: 20 }, () => testSchema.connect()))
      const results = (
        await Promise.all(
          conns.map(async (c, ci) => {
            const out = []
            for (let i = ci; i < 100; i += 20)
              out.push(
                await enqueue(c, testSchema.table, {
                  task: "t",
                  key: "k",
                  payload: i,
                  mode,
                  delay: 2,
                  maxWait: 10,
                  now: T(i / 100),
                }),
              )
            return out
          }),
        )
      ).flat()
      expect(results.filter(r => r.created)).toHaveLength(1)
      expect(new Set(results.map(r => r.id)).size).toBe(1)
      const rows = await jobs(testSchema.conn.query)
      expect(rows).toHaveLength(1)
      if (mode === "debounce") {
        // Monotonic: run_at is not before the latest event + delay, and not after the window.
        expect(secondsOf(rows[0]!.runAt)).toBeCloseTo(0.99 + 2, 3)
      }
    })
  }

  test("a second producer waits for an uncommitted insert and merges into it after the commit", async () => {
    const p1 = await testSchema.connect()
    const p2 = await testSchema.connect()
    await p1.query`begin`
    const a = await enqueue(p1, testSchema.table, { task: "t", key: "k", payload: 1, now: T(0) })
    const second = enqueue(p2, testSchema.table, { task: "t", key: "k", payload: 2, now: T(0) })
    await waitForLock(testSchema.conn, p2.pid)
    await p1.query`commit`
    expect(await second).toEqual({ id: a.id, created: false })
    expect((await jobs(testSchema.conn.query))[0]!.payload).toBe(2)
  })

  test("a second producer inserts its own row when the first rolls back", async () => {
    const p1 = await testSchema.connect()
    const p2 = await testSchema.connect()
    await p1.query`begin`
    await enqueue(p1, testSchema.table, { task: "t", key: "k", payload: 1, now: T(0) })
    const second = enqueue(p2, testSchema.table, { task: "t", key: "k", payload: 2, now: T(0) })
    await waitForLock(testSchema.conn, p2.pid)
    await p1.query`rollback`
    expect((await second).created).toBe(true)
    expect((await jobs(testSchema.conn.query)).map(r => r.payload)).toEqual([2])
  })
})

describe("event during active", () => {
  test("creates a second pending row; the active job keeps its key and completes", async () => {
    await enqueue(testSchema.conn, testSchema.table, { task: "t", key: "k", payload: 1, now: T(0) })
    const claim = only(await claimPlain(testSchema.conn.query, { ...W, limit: 1, now: T(0) }))
    const b = await enqueue(testSchema.conn, testSchema.table, {
      task: "t",
      key: "k",
      payload: 2,
      mode: "keep",
      now: T(1),
    })
    expect(b.created).toBe(true)
    const rows = await jobs(testSchema.conn.query)
    expect(rows.map(r => [r.state, r.key, r.payload])).toEqual([
      ["active", "k", 1],
      ["pending", "k", 2],
    ])
    expect(await complete(testSchema.db, claim)).toBe(true)
    expect((await jobs(testSchema.conn.query)).map(r => r.id)).toEqual([b.id])
  })

  test("the next pending row of a key starts while the active job runs", async () => {
    await enqueue(testSchema.conn, testSchema.table, { task: "t", key: "k", payload: 1, now: T(0) })
    only(await claimPlain(testSchema.conn.query, { ...W, limit: 1, now: T(0) }))
    await enqueue(testSchema.conn, testSchema.table, { task: "t", key: "k", payload: 2, now: T(1) })
    only(await claimPlain(testSchema.conn.query, { ...W, limit: 1, now: T(1) }))
    expect((await jobs(testSchema.conn.query)).map(r => [r.state, r.key, r.payload])).toEqual([
      ["active", "k", 1],
      ["active", "k", 2],
    ])
  })

  test("a producer blocked by an uncommitted claim inserts a new row after the claim commits", async () => {
    await enqueue(testSchema.conn, testSchema.table, { task: "t", key: "k", payload: 1, now: T(0) })
    const worker = await testSchema.connect()
    const producer = await testSchema.connect()
    await worker.query`begin`
    const claim = only(await claimPlain(worker.query, { ...W, limit: 1, now: T(0) }))
    const event = enqueue(producer, testSchema.table, {
      task: "t",
      key: "k",
      payload: 2,
      now: T(0),
    })
    await waitForLock(testSchema.conn, producer.pid)
    await worker.query`commit`
    const r = await event
    expect(r.created).toBe(true)
    expect(r.id).not.toBe(claim.id)
    expect((await jobs(testSchema.conn.query)).map(r => [r.state, r.payload])).toEqual([
      ["active", 1],
      ["pending", 2],
    ])
  })
})

describe("an uncommitted enqueue keeps the pending row from being claimed", () => {
  // Without the row lock, the job would start before the transaction of the event commits and miss its data.
  for (const mode of ["replace", "keep", "debounce"] as KeyMode[]) {
    test(mode, async () => {
      await enqueue(testSchema.conn, testSchema.table, {
        task: "t",
        key: "k",
        payload: 1,
        mode,
        delay: 0,
        maxWait: 10,
        now: T(0),
      })
      const producer = await testSchema.connect()
      await producer.query`begin`
      await enqueue(producer, testSchema.table, {
        task: "t",
        key: "k",
        payload: 2,
        mode,
        delay: 0,
        maxWait: 10,
        now: T(1),
      })
      expect(await claimPlain(testSchema.conn.query, { ...W, limit: 1, now: T(5) })).toEqual([])
      await producer.query`commit`
      expect(await claimPlain(testSchema.conn.query, { ...W, limit: 1, now: T(5) })).toHaveLength(1)
    })
  }

  test("claiming without a queue does not block on the row of the producer", async () => {
    await enqueue(testSchema.conn, testSchema.table, { task: "t", key: "k", now: T(0) })
    const producer = await testSchema.connect()
    await producer.query`begin`
    await enqueue(producer, testSchema.table, { task: "t", key: "k", now: T(0) })
    const claim = claimPlain(testSchema.conn.query, { ...W, limit: 1, now: T(1) })
    expect(await isPending(claim, 200)).toBe(false)
    await producer.query`rollback`
  })
})

const DEBOUNCE = { delay: 2, maxWait: 10 }

interface Execution {
  id: string
  key: string
  start: number
  events: number[]
}

/** Log of events and executions that checks the invariants of debounce. */
class Ledger {
  events: { key: string; at: number; id: string }[] = []
  executions: Execution[] = []
  failedAt = new Map<string, number[]>()

  event(key: string, at: number, id: string) {
    this.events.push({ key, at, id })
  }

  execute(claim: Claim, at: number) {
    const events = this.events.filter(e => e.id === claim.id).map(e => e.at)
    this.executions.push({ id: claim.id, key: claim.job.key!, start: at, events })
  }

  /**
   * Checks the guarantees of debounce:
   * - a start is not later than the first event of the row + maxWait (+ `lateTolerance`, or not checked when it is `null`);
   * - a start is not earlier than min(last event + delay, first event + maxWait);
   * - every event is covered by an execution that started not earlier than the event.
   * After a failure of the row, its window restarts at the failure: the earliest start counts from the failure and the events after it, and the latest start is not checked, since the retry waits for its backoff, which can pass maxWait.
   */
  check({ lateTolerance = 0 as number | null } = {}) {
    for (const x of this.executions) {
      const fails = (this.failedAt.get(x.id) ?? []).filter(f => f < x.start)
      const since = fails.length ? Math.max(...fails) : -Infinity
      const own = x.events.filter(e => e <= x.start && e >= since)
      if (own.length === 0) continue
      const first = fails.length ? since : Math.min(...own)
      const last = Math.max(...own)
      if (!fails.length && lateTolerance != null)
        expect(x.start).toBeLessThanOrEqual(first + DEBOUNCE.maxWait + lateTolerance)
      expect(x.start).toBeGreaterThanOrEqual(
        Math.min(last + DEBOUNCE.delay, first + DEBOUNCE.maxWait) - 1e-9,
      )
    }
    for (const e of this.events) {
      const covered = this.executions.some(x => x.id === e.id && x.start >= e.at)
      expect({ event: e, covered }).toEqual({ event: e, covered: true })
    }
  }
}

/** Runs ticks of 0.5 s: on a tick the event comes first, then the claim; an execution is instant. */
async function runTicks(
  conn: Conn,
  o: {
    events: (tick: number) => boolean
    until: number
    order?: (tick: number) => "event-first" | "claim-first"
    onClaim?: (claim: Claim, tick: number) => Promise<"done" | "handled">
    ledger?: Ledger
  },
) {
  const ledger = o.ledger ?? new Ledger()
  for (let i = 0; i * 0.5 <= o.until; i++) {
    const tick = i * 0.5
    const doEvent = async () => {
      if (!o.events(tick)) return
      const r = await enqueue(conn, testSchema.table, {
        task: "t",
        key: "k",
        mode: "debounce",
        ...DEBOUNCE,
        payload: tick,
        now: T(tick),
      })
      ledger.event("k", tick, r.id)
    }
    const doClaim = async () => {
      for (const claim of await claimPlain(conn.query, { ...W, limit: 10, now: T(tick) })) {
        const r = (await o.onClaim?.(claim, tick)) ?? "done"
        if (r === "done") {
          ledger.execute(claim, tick)
          expect(await complete(testSchema.db, claim)).toBe(true)
        }
      }
    }
    if ((o.order?.(tick) ?? "event-first") === "event-first") {
      await doEvent()
      await doClaim()
    } else {
      await doClaim()
      await doEvent()
    }
  }
  return ledger
}

describe("debounce on virtual time", () => {
  test("50 events every 0.5 s, delay 2, maxWait 10: three runs at 10, 20.5, 26.5", async () => {
    const ledger = await runTicks(testSchema.conn, { events: t => t <= 24.5, until: 40 })
    expect(ledger.executions.map(x => x.start)).toEqual([10, 20.5, 26.5])
    expect(ledger.executions.map(x => x.events.length)).toEqual([21, 21, 8])
    ledger.check()
  })

  test("an event exactly at the window end before the claim joins the current window", async () => {
    const ledger = await runTicks(testSchema.conn, { events: t => t <= 10, until: 20 })
    expect(ledger.executions.map(x => [x.start, Math.max(...x.events)])).toEqual([[10, 10]])
    ledger.check()
  })

  test("an event exactly at the window end after the claim opens a new window", async () => {
    const ledger = await runTicks(testSchema.conn, {
      events: t => t <= 10,
      until: 20,
      order: t => (t === 10 ? "claim-first" : "event-first"),
    })
    expect(ledger.executions.map(x => [x.start, x.events])).toEqual([
      [10, Array.from({ length: 20 }, (_, i) => i / 2)],
      [12, [10]],
    ])
    ledger.check()
  })

  test("an event at the run_at of its delay before the claim postpones the run", async () => {
    // Events at 0 and 2: run_at after the first is 2, and the event at tick 2 moves it to 4.
    const ledger = await runTicks(testSchema.conn, { events: t => t === 0 || t === 2, until: 10 })
    expect(ledger.executions.map(x => [x.start, x.events])).toEqual([[4, [0, 2]]])
    ledger.check()
  })

  test("a failure with backoff 30 at tick 10 under a stream of events: a run at 40, then a new window", async () => {
    const ledger = new Ledger()
    let failed = false
    await runTicks(testSchema.conn, {
      events: t => t <= 50,
      until: 70,
      ledger,
      onClaim: async (claim, tick) => {
        if (failed) return "done"
        failed = true
        expect(
          await fail(testSchema.db, claim, {
            error: "boom",
            backoff: 30,
            now: T(tick),
          }),
        ).toBe("retry")
        ledger.failedAt.set(claim.id, [tick])
        return "handled"
      },
    })
    expect(ledger.executions.map(x => x.start)).toEqual([40, 50.5])
    ledger.check()
  })
})

describe("debounce: maxWait bounds the computed deadline, not the claim", () => {
  test("a long producer transaction holding the due row delays the claim past maxWait; lag is reported", async () => {
    await enqueue(testSchema.conn, testSchema.table, {
      task: "t",
      key: "k",
      mode: "debounce",
      ...DEBOUNCE,
      now: T(0),
    })
    const producer = await testSchema.connect()
    await producer.query`begin`
    await enqueue(producer, testSchema.table, {
      task: "t",
      key: "k",
      mode: "debounce",
      ...DEBOUNCE,
      now: T(2),
    })
    for (let tick = 2; tick < 15; tick += 0.5)
      expect(await claimPlain(testSchema.conn.query, { ...W, limit: 1, now: T(tick) })).toEqual([])
    await producer.query`commit`
    const claim = only(await claimPlain(testSchema.conn.query, { ...W, limit: 1, now: T(15) }))
    // Deadline min(2 + 2, 0 + 10) = 4 respected; the claim is 11 s late, 5 s past window start + maxWait.
    expect([secondsOf(only(await jobs(testSchema.conn.query)).runAt), claim.lag]).toEqual([4, 11])
  })
})

describe("debounce: maxWait below delay", () => {
  test("the first event is capped by maxWait, not only the following ones", async () => {
    await enqueue(testSchema.conn, testSchema.table, {
      task: "t",
      key: "k",
      mode: "debounce",
      delay: 5,
      maxWait: 2,
      now: T(0),
    })
    expect(secondsOf((await jobs(testSchema.conn.query))[0]!.runAt)).toBe(2)
  })
})

describe("debounce: changes of settings and key modes", () => {
  const debounce = (key: string, now: number, settings: { delay: number; maxWait: number }) =>
    enqueue(testSchema.conn, testSchema.table, {
      task: "t",
      key,
      mode: "debounce",
      ...settings,
      now: T(now),
    })
  const runAt = async (key: string) =>
    secondsOf(only(await jobs(testSchema.conn.query, sql`key = ${key}`)).runAt)

  test("a later event never moves run_at back, also with a smaller maxWait, and the commit order does not change it", async () => {
    await debounce("in-order", 0, { delay: 8, maxWait: 3 })
    expect(await runAt("in-order")).toBe(3)
    await debounce("in-order", 1, { delay: 8, maxWait: 10 })
    expect(await runAt("in-order")).toBe(9)
    await debounce("in-order", 2, { delay: 8, maxWait: 3 })
    expect(await runAt("in-order")).toBe(9)
    // The same events, the last two committed in reverse order.
    await debounce("reversed", 0, { delay: 8, maxWait: 3 })
    await debounce("reversed", 2, { delay: 8, maxWait: 3 })
    expect(await runAt("reversed")).toBe(3)
    await debounce("reversed", 1, { delay: 8, maxWait: 10 })
    expect(await runAt("reversed")).toBe(9)
  })

  test("a debounce enqueue over a pending job of another mode opens a new window", async () => {
    await enqueue(testSchema.conn, testSchema.table, { task: "t", key: "k", delay: 100, now: T(0) })
    await debounce("k", 50, DEBOUNCE)
    const row = only(await jobs(testSchema.conn.query))
    expect([secondsOf(row.runAt), secondsOf(row.windowStartedAt)]).toEqual([52, 50])
    await debounce("k", 59, DEBOUNCE)
    expect(await runAt("k")).toBe(60)
  })

  test("an enqueue in another mode ends the window", async () => {
    await debounce("k", 0, { delay: 8, maxWait: 10 })
    await enqueue(testSchema.conn, testSchema.table, {
      task: "t",
      key: "k",
      mode: "keep",
      now: T(1),
    })
    expect(await runAt("k")).toBe(8)
    await debounce("k", 2, DEBOUNCE)
    const row = only(await jobs(testSchema.conn.query))
    expect([secondsOf(row.runAt), secondsOf(row.windowStartedAt)]).toEqual([4, 2])
  })
})

describe("debounce under concurrency", () => {
  test("a producer with an earlier now that commits later does not move run_at back", async () => {
    // In production, now of a producer is the start of its statement, and commits arrive in another order.
    await enqueue(testSchema.conn, testSchema.table, {
      task: "t",
      key: "k",
      mode: "debounce",
      ...DEBOUNCE,
      now: T(0),
    })
    await enqueue(testSchema.conn, testSchema.table, {
      task: "t",
      key: "k",
      mode: "debounce",
      ...DEBOUNCE,
      now: T(6),
    })
    await enqueue(testSchema.conn, testSchema.table, {
      task: "t",
      key: "k",
      mode: "debounce",
      ...DEBOUNCE,
      now: T(5),
    })
    expect(secondsOf((await jobs(testSchema.conn.query))[0]!.runAt)).toBe(8)
    // A formula without monotonicity would give 7, a run 1 s after the event at 6.
  })

  test("a producer with an earlier now that joins a later window counts maxWait from the window", async () => {
    await enqueue(testSchema.conn, testSchema.table, {
      task: "t",
      key: "k",
      mode: "debounce",
      delay: 0,
      maxWait: 2,
      now: T(10),
    })
    await enqueue(testSchema.conn, testSchema.table, {
      task: "t",
      key: "k",
      mode: "debounce",
      delay: 10,
      maxWait: 2,
      now: T(9),
    })
    expect(secondsOf(only(await jobs(testSchema.conn.query)).runAt)).toBe(12)
    // A cap from the now of the producer would give 9 + 2 = 11.
  })

  test("an event holds the row in an open transaction: the claim at the deadline skips it and takes it with the event after the commit", async () => {
    await enqueue(testSchema.conn, testSchema.table, {
      task: "t",
      key: "k",
      mode: "debounce",
      ...DEBOUNCE,
      now: T(0),
    })
    await enqueue(testSchema.conn, testSchema.table, {
      task: "t",
      key: "k",
      mode: "debounce",
      ...DEBOUNCE,
      now: T(9),
    })
    const producer = await testSchema.connect()
    await producer.query`begin`
    await enqueue(producer, testSchema.table, {
      task: "t",
      key: "k",
      mode: "debounce",
      ...DEBOUNCE,
      payload: "edge",
      now: T(10),
    })
    expect(await claimPlain(testSchema.conn.query, { ...W, limit: 1, now: T(10) })).toEqual([])
    await producer.query`commit`
    const claim = only(await claimPlain(testSchema.conn.query, { ...W, limit: 1, now: T(10) }))
    expect(claim.job.payload).toBe("edge")
  })

  test("a claim holds the row in an open transaction: the event waits and opens a new window after the commit", async () => {
    await enqueue(testSchema.conn, testSchema.table, {
      task: "t",
      key: "k",
      mode: "debounce",
      ...DEBOUNCE,
      now: T(0),
    })
    const worker = await testSchema.connect()
    const producer = await testSchema.connect()
    await worker.query`begin`
    await claimPlain(worker.query, { ...W, limit: 1, now: T(10) })
    const event = enqueue(producer, testSchema.table, {
      task: "t",
      key: "k",
      mode: "debounce",
      ...DEBOUNCE,
      now: T(10),
    })
    await waitForLock(testSchema.conn, producer.pid)
    await worker.query`commit`
    expect((await event).created).toBe(true)
    const rows = await jobs(testSchema.conn.query, sql`state = 'pending'`)
    expect([secondsOf(rows[0]!.windowStartedAt), secondsOf(rows[0]!.runAt)]).toEqual([10, 12])
  })

  test("8 producers and a worker race on every tick, 10 keys, 300 ticks: no start is early, every event is covered, and the lateness past maxWait is measured", async () => {
    const producers = await Promise.all(Array.from({ length: 8 }, () => testSchema.connect()))
    const worker = await testSchema.connect()
    const ledger = new Ledger()
    const random = createRandom(42)
    const keyOf = (claim: Claim) => claim.job.key!
    for (let i = 0; i <= 300; i++) {
      const tick = i * 0.5
      const burst = tick <= 120
      await Promise.all([
        ...producers.map(async p => {
          if (!burst || random.float() < 0.5) return
          const key = `k${random.int(0, 9)}`
          const r = await enqueue(p, testSchema.table, {
            task: "t",
            key,
            mode: "debounce",
            ...DEBOUNCE,
            now: T(tick),
          })
          ledger.event(key, tick, r.id)
        }),
        (async () => {
          for (const claim of await claimPlain(worker.query, {
            ...W,
            limit: 100,
            now: T(tick),
          })) {
            ledger.executions.push({ id: claim.id, key: keyOf(claim), start: tick, events: [] })
            await complete(testSchema.db, claim)
          }
        })(),
      ])
    }
    for (const x of ledger.executions)
      x.events = ledger.events.filter(e => e.id === x.id).map(e => e.at)
    // A start past maxWait is not asserted: a due row locked by a producer is skipped (SKIP LOCKED), see the starvation test.
    ledger.check({ lateTolerance: null })
    const lateness = ledger.executions
      .map(x => x.start - (Math.min(...x.events) + DEBOUNCE.maxWait))
      .filter(d => d > 0)
    console.log(
      `debounce concurrent: ${ledger.events.length} events, ${ledger.executions.length} executions, ` +
        `${lateness.length} past maxWait, max lateness ${Math.max(0, ...lateness)} s`,
    )
    expect(await jobs(testSchema.conn.query)).toEqual([])
  })

  test("a producer holding the due row at every poll starves it past maxWait", async () => {
    await enqueue(testSchema.conn, testSchema.table, {
      task: "t",
      key: "k",
      mode: "debounce",
      ...DEBOUNCE,
      now: T(0),
    })
    const producer = await testSchema.connect()
    for (let tick = 10; tick <= 15; tick += 0.5) {
      await producer.query`begin`
      await enqueue(producer, testSchema.table, {
        task: "t",
        key: "k",
        mode: "debounce",
        ...DEBOUNCE,
        now: T(tick),
      })
      expect(await claimPlain(testSchema.conn.query, { ...W, limit: 1, now: T(tick) })).toEqual([])
      await producer.query`commit`
    }
    const row = only(await jobs(testSchema.conn.query))
    expect([row.state, secondsOf(row.runAt)]).toEqual(["pending", 10])
  })
})
