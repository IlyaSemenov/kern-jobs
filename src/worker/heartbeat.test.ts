// Heartbeat of a worker on a fake connection: one heartbeat at a time, health, reopening its session, the lock of its session, stopping, lost and busy claims, and local deadlines.

import { afterEach, expect, jest, test } from "bun:test"

import {
  type Connection,
  ConnectionError,
  createDatabase,
  type DriverResult,
  QueryError,
  queryOn,
  type TextValue,
} from "kern-orm/db"

import type { ClaimRef } from "./db/claim"
import { bindWorkerDb } from "./db/db"
import * as t from "./db/types"
import type { WorkerEvent } from "./events"
import { HandlerAbort, type Heartbeat, startHeartbeat } from "./heartbeat"
import { monotonicNow, sleep } from "./loop"
import { createAcquireOnlyDriver } from "./test-harness"

/** Row of a result of the fake pool: the text of each column by its name, or null. */
type TextRow = Record<string, string | null>

interface Call {
  text: string
  /** Parameters as the driver gets them, such as `{"c1","c2"}` for an array. */
  params: readonly TextValue[]
  settled: boolean
  /** Answers the statement with rows by column name. */
  resolve(rows: TextRow[]): void
  reject(error: unknown): void
}

/** OID of the PostgreSQL type `text`. */
const textOid = 25

/** Converts rows by column name into a result of a driver, with the columns of the first row as `text`. */
function toResult(rows: TextRow[]): DriverResult {
  const names = rows[0] ? Object.keys(rows[0]) : []
  return {
    command: "SELECT",
    count: rows.length,
    columns: names.map(name => ({ name, type: textOid })),
    rows: rows.map(row => names.map(name => row[name] ?? null)),
  }
}

/**
 * Pool whose connections hand every statement to the test, which answers it, except the statements of advisory locks, which it answers itself: a lock once `lockGate` resolves, taken unless `lockBusy` is set, and an unlock at once.
 * `release` of a connection records its number, counted from 1 in the order of acquiring, and returns what `onRelease` returns.
 * `journal` records the locks, unlocks, and releases in their order, with the number of their connection.
 */
function createFakeSource(onRelease?: () => Promise<void>) {
  const calls: Call[] = []
  const releases: number[] = []
  const journal: string[] = []
  let acquires = 0
  let waiters: (() => void)[] = []
  const source = {
    async acquire(): Promise<Connection> {
      const number = ++acquires
      return {
        execute: ({ text, parameters }) => {
          if (text.includes("pg_try_advisory_lock")) {
            journal.push(`lock ${number}`)
            const taken = fake.lockBusy ? "f" : "t"
            return fake.lockGate.then(() => toResult([{ taken }]))
          }
          if (text.includes("pg_advisory_unlock")) {
            journal.push(`unlock ${number}`)
            return Promise.resolve(toResult([{ released: "t" }]))
          }
          return new Promise<DriverResult>((resolve, reject) => {
            const c: Call = {
              text,
              params: parameters,
              settled: false,
              resolve: rows => ((c.settled = true), resolve(toResult(rows))),
              reject: error => ((c.settled = true), reject(error)),
            }
            calls.push(c)
            for (const w of waiters) w()
            waiters = []
          })
        },
        executeScript: async () => {},
        release: () => {
          releases.push(number)
          journal.push(`release ${number}`)
          return onRelease?.()
        },
      }
    },
  }
  /** Waits for the call with this index. */
  const call = async (index: number): Promise<Call> => {
    while (calls.length <= index) await new Promise<void>(r => waiters.push(r))
    return calls[index]!
  }
  const fake = {
    source,
    calls,
    releases,
    journal,
    lockBusy: false,
    lockGate: Promise.resolve(),
    call,
    acquires: () => acquires,
  }
  fakes.push(fake)
  return fake
}

const fakes: { calls: Call[] }[] = []

const claim = (id: string): ClaimRef => ({ id, claimId: `c${id}` })
const isHeartbeat = (c: Call) => c.text.includes("unnest")
/** Answers a heartbeat: the claims extended, and the claims busy. */
const answer = (c: Call, extended: string[], busy: string[] = []) =>
  c.resolve([
    ...extended.map(claimId => ({ claim_id: claimId, extended: "t" })),
    ...busy.map(claimId => ({ claim_id: claimId, extended: "f" })),
  ])

let heartbeat: Heartbeat | undefined
afterEach(async () => {
  // Stop waits for the heartbeat in flight, which the test may have left unanswered.
  let stopped = false
  const stopping = heartbeat?.stop().then(() => (stopped = true))
  while (heartbeat && !stopped) {
    for (const fake of fakes) for (const c of fake.calls) if (!c.settled) c.resolve([])
    await sleep(0.01)
  }
  await stopping
  heartbeat = undefined
  fakes.length = 0
})

function start(
  source: ReturnType<typeof createFakeSource>["source"],
  events: WorkerEvent[] = [],
  onSuccess = () => {},
) {
  heartbeat = startHeartbeat({
    db: bindWorkerDb(
      createDatabase({
        driver: createAcquireOnlyDriver(() => source.acquire()),
        onReleaseError: error => events.push({ type: "release-error", error }),
      }),
      "fake",
    ),
    lock: { key: "worker", busyMessage: "worker id in use" },
    interval: 0.02,
    margin: 0.1,
    onEvent: e => events.push(e),
    onSuccess,
  })
  return heartbeat
}

test("sends one heartbeat at a time, however long one takes", async () => {
  const fake = createFakeSource()
  const hb = start(fake.source)
  hb.keep(claim("1"), 10, monotonicNow())
  let first = await fake.call(0)
  if (!isHeartbeat(first)) {
    first.resolve([])
    first = await fake.call(1)
  }
  const sent = fake.calls.length
  await sleep(0.2)
  expect(fake.calls.length).toBe(sent)
  answer(first, ["c1"])
  await fake.call(sent)
})

test("is healthy after a heartbeat succeeded, and not while one is in flight longer than an interval", async () => {
  const fake = createFakeSource()
  const hb = start(fake.source)
  expect(hb.healthy()).toBe(false)
  ;(await fake.call(0)).resolve([])
  const next = await fake.call(1)
  expect(hb.healthy()).toBe(true)
  await sleep(0.05)
  expect(hb.healthy()).toBe(false)
  next.resolve([])
  await fake.call(2)
  expect(hb.healthy()).toBe(true)
})

test("is healthy when it reports the success of a heartbeat that took longer than an interval", async () => {
  const fake = createFakeSource()
  const seen: boolean[] = []
  const hb = start(fake.source, [], () => seen.push(hb.healthy()))
  const slow = await fake.call(0)
  await sleep(0.05)
  slow.resolve([])
  await fake.call(1)
  expect(seen).toEqual([true])
})

test("a connection error replaces the heartbeat connection; a rejected statement keeps it", async () => {
  const fake = createFakeSource()
  const events: WorkerEvent[] = []
  const hb = start(fake.source, events)
  const lost = new ConnectionError("reset")
  ;(await fake.call(0)).reject(lost)
  ;(await fake.call(1)).reject(new QueryError("timeout", { code: "57014" }))
  ;(await fake.call(2)).resolve([])
  await fake.call(3)
  expect([fake.acquires(), fake.releases]).toEqual([2, [1]])
  expect(events.map(e => e.type)).toEqual([
    "heartbeat-error",
    "heartbeat-error",
    "heartbeat-recovered",
  ])
  expect(hb.healthy()).toBe(true)
})

test("a release that fails is reported, and the next heartbeat acquires another connection", async () => {
  const releaseError = new Error("release failed")
  const fake = createFakeSource(async () => {
    throw releaseError
  })
  const events: WorkerEvent[] = []
  start(fake.source, events)
  const lost = new ConnectionError("reset")
  ;(await fake.call(0)).reject(lost)
  ;(await fake.call(1)).resolve([])
  await fake.call(2)
  expect([fake.acquires(), fake.releases]).toEqual([2, [1]])
  expect(events).toEqual([
    { type: "heartbeat-error", error: lost },
    { type: "release-error", error: releaseError },
    { type: "heartbeat-recovered" },
  ])
})

test("a session takes the lock before its first heartbeat and holds it, and a new session after a connection error takes it again", async () => {
  const fake = createFakeSource()
  start(fake.source)
  ;(await fake.call(0)).resolve([])
  expect(fake.journal).toEqual(["lock 1"])
  ;(await fake.call(1)).reject(new ConnectionError("reset"))
  ;(await fake.call(2)).resolve([])
  expect(fake.journal).toEqual(["lock 1", "unlock 1", "release 1", "lock 2"])
})

test("a lock that another session holds fails the round and closes its session, and the heartbeat is healthy once a later round takes it", async () => {
  const fake = createFakeSource()
  fake.lockBusy = true
  const events: WorkerEvent[] = []
  const hb = start(fake.source, events)
  while (fake.releases.length < 2) await sleep(0.01)
  expect([fake.calls.length, hb.healthy()]).toEqual([0, false])
  expect(events.slice(0, 2)).toEqual([
    { type: "heartbeat-error", error: new Error("worker id in use") },
    { type: "heartbeat-error", error: new Error("worker id in use") },
  ])
  fake.lockBusy = false
  ;(await fake.call(0)).resolve([])
  await fake.call(1)
  expect(hb.healthy()).toBe(true)
  expect(events.at(-1)).toEqual({ type: "heartbeat-recovered" })
})

test("stop releases the lock before the connection", async () => {
  const fake = createFakeSource()
  const hb = start(fake.source)
  ;(await fake.call(0)).resolve([])
  const inFlight = await fake.call(1)
  const stopping = hb.stop()
  inFlight.resolve([])
  await stopping
  heartbeat = undefined
  expect(fake.journal).toEqual(["lock 1", "unlock 1", "release 1"])
})

test("a stop that begins while the lock is taken sends no heartbeat, and releases the lock and the connection", async () => {
  const fake = createFakeSource()
  const gate = Promise.withResolvers<void>()
  fake.lockGate = gate.promise
  const hb = start(fake.source)
  while (fake.journal.length === 0) await sleep(0.01)
  const stopping = hb.stop()
  gate.resolve()
  await stopping
  heartbeat = undefined
  expect([fake.calls.length, fake.journal]).toEqual([0, ["lock 1", "unlock 1", "release 1"]])
})

test("runLocked runs its statements in the session that holds the lock, and rejects once the stop began", async () => {
  const fake = createFakeSource()
  const hb = start(fake.source)
  ;(await fake.call(0)).resolve([])
  const ran = hb.runLocked(
    async target => await queryOn(target)`select 42 as answer`.get(t.integer),
  )
  let index = 1
  let call = await fake.call(index)
  while (!call.text.includes("42")) {
    call.resolve([])
    call = await fake.call(++index)
  }
  call.resolve([{ answer: "42" }])
  expect(await ran).toBe(42)
  expect([fake.acquires(), fake.journal]).toEqual([1, ["lock 1"]])
  void hb.stop()
  await expect(hb.runLocked(async () => {})).rejects.toThrow("The heartbeat holds no lock")
})

test("stop waits for the release of the connection", async () => {
  const released = Promise.withResolvers<void>()
  const fake = createFakeSource(() => released.promise)
  const hb = start(fake.source)
  ;(await fake.call(0)).resolve([])
  const inFlight = await fake.call(1)
  let stopped = false
  const stopping = hb.stop().then(() => (stopped = true))
  inFlight.resolve([])
  await sleep(0.05)
  expect([stopped, fake.releases]).toEqual([false, [1]])
  released.resolve()
  await stopping
  heartbeat = undefined
})

test("a session that opens after the stop began sends no statement, and its connection is released once", async () => {
  const fake = createFakeSource()
  const acquiring = Promise.withResolvers<void>()
  const arrived = Promise.withResolvers<void>()
  const hb = start({
    async acquire() {
      acquiring.resolve()
      await arrived.promise
      return await fake.source.acquire()
    },
  })
  await acquiring.promise
  const stopping = hb.stop()
  arrived.resolve()
  await stopping
  heartbeat = undefined
  expect([fake.calls.length, fake.releases]).toEqual([0, [1]])
})

test("a lost claim is aborted and no longer sent; a busy claim is neither confirmed nor lost", async () => {
  const fake = createFakeSource()
  const events: WorkerEvent[] = []
  const hb = start(fake.source, events)
  const [a, b, c] = ["1", "2", "3"].map(id => hb.keep(claim(id), 10, monotonicNow()))
  let index = 0
  let call = await fake.call(index)
  while (!isHeartbeat(call) || call.params[1]!.split(",").length < 3) {
    call.resolve([])
    call = await fake.call(++index)
  }
  answer(call, ["c1"], ["c2"])
  const next = await fake.call(index + 1)
  expect(next.params.slice(0, 2)).toEqual(['{"1","2"}', '{"c1","c2"}'])
  expect([a!.lost, b!.lost, c!.lost]).toEqual([false, false, true])
  expect((c!.signal.reason as HandlerAbort).reason).toBe("lost")
  expect([a!.signal.aborted, b!.signal.aborted]).toEqual([false, false])
  expect(events.filter(e => e.type === "abort")).toEqual([
    { type: "abort", claim: claim("3"), reason: "lost" },
  ])
})

test("a lease that is not confirmed is aborted a margin before its end; a confirmed one is not", async () => {
  const fake = createFakeSource()
  const hb = start(fake.source)
  const since = monotonicNow()
  const confirmed = hb.keep(claim("1"), 0.3, since)
  const busy = hb.keep(claim("2"), 0.3, since)
  let aborted = 0
  busy.signal.addEventListener("abort", () => (aborted = monotonicNow() - since))
  for (let i = 0; monotonicNow() - since < 500; i++) {
    const call = await fake.call(i)
    if (isHeartbeat(call)) answer(call, ["c1"], ["c2"])
    else call.resolve([])
  }
  expect((busy.signal.reason as HandlerAbort).reason).toBe("deadline")
  expect(aborted).toBeGreaterThanOrEqual(190)
  expect(aborted).toBeLessThan(300)
  expect(confirmed.signal.aborted).toBe(false)
})

test("a lease longer than the longest delay of a timer is aborted at its deadline, not when its first timer fires", () => {
  jest.useFakeTimers()
  try {
    const hb = start(createFakeSource().source)
    // 30 days, more than the 2^31 - 1 milliseconds of a timer.
    const lease = 30 * 24 * 3600
    const kept = hb.keep(claim("1"), lease, monotonicNow())
    const deadline = (lease - 0.1) * 1000
    jest.advanceTimersByTime(2 ** 31)
    expect(kept.signal.aborted).toBe(false)
    jest.advanceTimersByTime(deadline - 2 ** 31 - 10)
    expect(kept.signal.aborted).toBe(false)
    jest.advanceTimersByTime(10)
    expect((kept.signal.reason as HandlerAbort).reason).toBe("deadline")
  } finally {
    jest.useRealTimers()
  }
})

test("a lease whose deadline passed before it is kept is aborted at once", () => {
  const hb = start(createFakeSource().source)
  const expired = hb.keep(claim("1"), 0.05, monotonicNow() - 1000)
  expect((expired.signal.reason as HandlerAbort).reason).toBe("deadline")
})

test("after its handler ends, an unconfirmed lease is not aborted, and the heartbeat keeps it until it is released", async () => {
  const fake = createFakeSource()
  const events: WorkerEvent[] = []
  const hb = start(fake.source, events)
  const kept = hb.keep(claim("1"), 0.3, monotonicNow())
  kept.endHandler()
  const since = monotonicNow()
  let sent = 0
  for (let i = 0; monotonicNow() - since < 400; i++) {
    const call = await fake.call(i)
    if (isHeartbeat(call)) {
      sent++
      answer(call, [], ["c1"])
    } else call.resolve([])
  }
  kept.release()
  expect(kept.signal.aborted).toBe(false)
  expect(events.filter(e => e.type === "abort")).toEqual([])
  expect(sent).toBeGreaterThan(5)
})
