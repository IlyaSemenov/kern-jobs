// Reaper loop: recovery after connection errors and batches that follow each other.

import { afterEach, beforeEach, expect, test } from "bun:test"

import { enqueue } from "../sql/enqueue"
import {
  createTestDriver,
  createTestSchema,
  databaseUrl,
  jobs,
  sleep,
  type TestSchema,
} from "../test-harness/database"

import { claimPlain } from "./db/claim"
import type { WorkerEvent } from "./events"
import { proxyTo } from "./faults/proxy"
import { startReaper } from "./reaper"
import { recordEvents } from "./test-harness"

let testSchema: TestSchema
beforeEach(async () => {
  testSchema = await createTestSchema()
})
afterEach(async () => {
  await testSchema.dispose()
})

/** Claims `n` jobs for a dead worker whose leases expire at once. */
async function expire(n: number) {
  for (let i = 0; i < n; i++) await enqueue(testSchema.conn, testSchema.table, { task: "t" })
  await claimPlain(testSchema.conn.query, {
    worker: "dead",
    tasks: ["t"],
    limit: n,
    defaultLease: 0.01,
  })
  await sleep(20)
}

test("after connection errors, the reaper returns expired jobs once the server is reachable again", async () => {
  const { proxy, url } = await proxyTo(databaseUrl())
  const driver = createTestDriver(url, testSchema.name, { max: 1 })
  const events: WorkerEvent[] = []
  proxy.refuse = true
  proxy.cut()
  const reaper = startReaper(testSchema.db.$queryOn(driver), {
    interval: 0.02,
    limit: 100,
    onEvent: e => events.push(e),
  })
  await expire(1)
  while (events.filter(e => e.type === "reap-error").length < 3) await sleep(10)
  proxy.refuse = false
  while (!events.some(e => e.type === "reap")) await sleep(10)
  await reaper.stop()
  expect((await jobs(testSchema.conn.query)).map(r => r.state)).toEqual(["pending"])
  await driver.close()
  await proxy.close()
})

test("a full batch starts the next round at once", async () => {
  await expire(5)
  const events: WorkerEvent[] = []
  const reaper = startReaper(testSchema.db.$queryOn(testSchema.driver), {
    interval: 3600,
    limit: 2,
    onEvent: e => events.push(e),
  })
  while (events.filter(e => e.type === "reap").length < 3) await sleep(10)
  await reaper.stop()
  expect(events.map(e => (e.type === "reap" ? e.jobs.length : e.type))).toEqual([2, 2, 1])
})

test("a producer that holds a pending insert of an expired job's key does not stop the return of other expired jobs, and the reaper returns the job once the producer ends", async () => {
  const blocked = await enqueue(testSchema.conn, testSchema.table, { task: "t", key: "blocked" })
  const independent = await enqueue(testSchema.conn, testSchema.table, {
    task: "t",
    key: "independent",
  })
  await claimPlain(testSchema.conn.query, {
    worker: "dead",
    tasks: ["t"],
    limit: 2,
    defaultLease: 0.01,
  })
  await sleep(20)
  const producer = await testSchema.connect()
  await producer.query`begin`
  await enqueue(producer, testSchema.table, { task: "t", key: "blocked" })
  const log = recordEvents()
  const reaper = startReaper(testSchema.db.$queryOn(testSchema.driver), {
    interval: 0.05,
    limit: 100,
    onEvent: log.onEvent,
  })
  try {
    await log.waitFor("reap", e => e.jobs.some(j => j.id === independent.id))
    expect((await jobs(testSchema.conn.query)).map(r => [r.key, r.state])).toEqual([
      ["blocked", "active"],
      ["independent", "pending"],
    ])
    await producer.query`rollback`
    await log.waitFor("reap", e => e.jobs.some(j => j.id === blocked.id))
    expect((await jobs(testSchema.conn.query)).map(r => [r.key, r.state])).toEqual([
      ["blocked", "pending"],
      ["independent", "pending"],
    ])
    expect(log.all("reap-error")).toEqual([])
  } finally {
    await producer.query`rollback`
    await reaper.stop()
  }
})
