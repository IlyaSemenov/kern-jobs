// Reaper loop: recovery after connection errors and batches that follow each other.
import { afterEach, beforeEach, expect, test } from "bun:test"

import { claimPlain } from "../core/claim"
import { enqueue } from "../core/enqueue"
import { query } from "../core/statement"
import {
  createTestDriver,
  createTestSchema,
  databaseUrl,
  jobs,
  sleep,
  type TestSchema,
} from "../core/testing"

import type { WorkerEvent } from "./events"
import { proxyTo } from "./faults/proxy"
import { startReaper } from "./reaper"
import { recordEvents } from "./testing"

let schema: TestSchema
beforeEach(async () => {
  schema = await createTestSchema()
})
afterEach(async () => {
  await schema.dispose()
})

/** Claims `n` jobs for a dead worker whose leases expire at once. */
async function expire(n: number) {
  for (let i = 0; i < n; i++) await enqueue(schema.admin, schema.table, { task: "t" })
  await claimPlain(schema.admin, schema.table, {
    worker: "dead",
    tasks: ["t"],
    limit: n,
    defaultLease: 0.01,
  })
  await sleep(20)
}

test("after connection errors, the reaper returns expired jobs once the server is reachable again", async () => {
  const { proxy, url } = await proxyTo(databaseUrl())
  const driver = createTestDriver(url, schema.name, { max: 1 })
  const events: WorkerEvent[] = []
  proxy.refuse = true
  proxy.cut()
  const reaper = startReaper(driver, schema.table, {
    interval: 0.02,
    limit: 100,
    onEvent: e => events.push(e),
  })
  await expire(1)
  while (events.filter(e => e.type === "reap-error").length < 3) await sleep(10)
  proxy.refuse = false
  while (!events.some(e => e.type === "reap")) await sleep(10)
  await reaper.stop()
  expect((await jobs(schema)).map(r => r.state)).toEqual(["pending"])
  await driver.close()
  await proxy.close()
})

test("a full batch starts the next round at once", async () => {
  await expire(5)
  const events: WorkerEvent[] = []
  const reaper = startReaper(schema.driver, schema.table, {
    interval: 3600,
    limit: 2,
    onEvent: e => events.push(e),
  })
  while (events.filter(e => e.type === "reap").length < 3) await sleep(10)
  await reaper.stop()
  expect(events.map(e => (e.type === "reap" ? e.jobs.length : e.type))).toEqual([2, 2, 1])
})

test("a producer that holds a pending insert of an expired job's key does not stop the return of other expired jobs, and the reaper returns the job once the producer ends", async () => {
  const blocked = await enqueue(schema.admin, schema.table, { task: "t", key: "blocked" })
  const independent = await enqueue(schema.admin, schema.table, { task: "t", key: "independent" })
  await claimPlain(schema.admin, schema.table, {
    worker: "dead",
    tasks: ["t"],
    limit: 2,
    defaultLease: 0.01,
  })
  await sleep(20)
  const producer = await schema.connect()
  await query(producer, "begin")
  await enqueue(producer, schema.table, { task: "t", key: "blocked" })
  const log = recordEvents()
  const reaper = startReaper(schema.driver, schema.table, {
    interval: 0.05,
    limit: 100,
    onEvent: log.onEvent,
  })
  try {
    await log.waitFor("reap", e => e.jobs.some(j => j.id === independent.id))
    expect((await jobs(schema)).map(r => [r.key, r.state])).toEqual([
      ["blocked", "active"],
      ["independent", "pending"],
    ])
    await query(producer, "rollback")
    await log.waitFor("reap", e => e.jobs.some(j => j.id === blocked.id))
    expect((await jobs(schema)).map(r => [r.key, r.state])).toEqual([
      ["blocked", "pending"],
      ["independent", "pending"],
    ])
    expect(log.all("reap-error")).toEqual([])
  } finally {
    await query(producer, "rollback")
    await reaper.stop()
  }
})
