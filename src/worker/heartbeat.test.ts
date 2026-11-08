// Heartbeat of a worker on a fake connection: one heartbeat at a time, health, reconnection, stopping, lost and busy claims, and local deadlines.
import { afterEach, expect, jest, test } from "bun:test"

import {
  type Connection,
  ConnectionError,
  type DriverResult,
  QueryError,
  type TextValue,
} from "kern-db"

import type { ClaimRef } from "../core/job"
import { createJobTable } from "../core/schema"
import type { TextRow } from "../core/statement"

import type { WorkerEvent } from "./events"
import { HandlerAbort, type Heartbeat, startHeartbeat } from "./heartbeat"
import { monotonicNow, sleep } from "./loop"

interface Call {
  text: string
  /** Parameters as the driver gets them, such as `{"c1","c2"}` for an array. */
  params: readonly TextValue[]
  settled: boolean
  /** Answers the statement with rows by column name. */
  resolve(rows: TextRow[]): void
  reject(error: unknown): void
}

/** Converts rows by column name into a result of a driver, with the columns of the first row. */
function toResult(rows: TextRow[]): DriverResult {
  const columns = rows[0] ? Object.keys(rows[0]) : []
  return {
    command: "SELECT",
    count: rows.length,
    columns,
    rows: rows.map(row => columns.map(column => row[column] ?? null)),
  }
}

/**
 * Pool whose connections hand every statement to the test, which answers it; `release` of a connection records its number, counted from 1 in the order of acquiring, and returns what `onRelease` returns.
 */
function createFakeSource(onRelease?: () => Promise<void>) {
  const calls: Call[] = []
  const releases: number[] = []
  let acquires = 0
  let waiters: (() => void)[] = []
  const source = {
    async acquire(): Promise<Connection> {
      const number = ++acquires
      return {
        execute: ({ text, parameters }) =>
          new Promise<DriverResult>((resolve, reject) => {
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
          }),
        executeScript: async () => {},
        release: () => {
          releases.push(number)
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
  const fake = { source, calls, releases, call, acquires: () => acquires }
  fakes.push(fake)
  return fake
}

const fakes: { calls: Call[] }[] = []

const claim = (id: string): ClaimRef => ({ id, claimId: `c${id}` })
const isHeartbeat = (c: Call) => c.text.includes("unnest")
/** Answers a heartbeat: the claims extended, and the claims busy. */
const answer = (c: Call, extended: string[], busy: string[] = []) =>
  c.resolve([
    ...extended.map(claimId => ({ claim_id: claimId, extended: "true" })),
    ...busy.map(claimId => ({ claim_id: claimId, extended: "false" })),
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

function start(source: ReturnType<typeof createFakeSource>["source"], events: WorkerEvent[] = []) {
  heartbeat = startHeartbeat({
    source,
    table: createJobTable("fake"),
    interval: 0.02,
    margin: 0.1,
    onEvent: e => events.push(e),
    onSuccess: () => {},
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

test("a connection error releases the connection and acquires another; a rejected statement keeps it", async () => {
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

test("a connection that arrives after the stop began sends no statement and is released once", async () => {
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
  while (!isHeartbeat(call) || call.params[2]!.split(",").length < 3) {
    call.resolve([])
    call = await fake.call(++index)
  }
  answer(call, ["c1"], ["c2"])
  const next = await fake.call(index + 1)
  expect(next.params.slice(1, 3)).toEqual(['{"1","2"}', '{"c1","c2"}'])
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
