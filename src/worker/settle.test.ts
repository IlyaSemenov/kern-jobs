// Settlement by the worker: retries after connection loss with known and unknown outcomes, a lost claim, rejected statements, and giving up past the confirmed lease.
import { afterEach, beforeEach, expect, test } from "bun:test"

import { ConnectionError, createDatabase, type Driver, QueryError } from "kern-db"

import { claimPlain } from "../core/claim"
import { enqueue } from "../core/enqueue"
import type { Claim } from "../core/job"
import {
  createTestDriver,
  createTestSchema,
  databaseUrl,
  jobs,
  only,
  type TestSchema,
} from "../core/testing"

import type { WorkerEvent } from "./events"
import { type Proxy, proxyTo } from "./faults/proxy"
import type { KeptLease } from "./heartbeat"
import { monotonicNow } from "./loop"
import { settle, type SettleOptions } from "./settle"

let schema: TestSchema
const proxies: Proxy[] = []
const drivers: Driver[] = []
beforeEach(async () => {
  schema = await createTestSchema()
})
afterEach(async () => {
  await Promise.all(drivers.splice(0).map(d => d.close()))
  await Promise.all(proxies.splice(0).map(p => p.close()))
  await schema.dispose()
})

/** Pool of the test schema through a proxy. */
async function proxied(): Promise<{ proxy: Proxy; driver: Driver }> {
  const { proxy, url } = await proxyTo(databaseUrl())
  proxies.push(proxy)
  const driver = createTestDriver(url, schema.name, { max: 2 })
  drivers.push(driver)
  return { proxy, driver }
}

async function claimJob(): Promise<Claim> {
  await enqueue(schema.admin, schema.table, { task: "t" })
  return only(
    await claimPlain(schema.admin, schema.table, {
      worker: "w",
      tasks: ["t"],
      limit: 1,
      defaultLease: 60,
    }),
  )
}

/** Lease kept by a heartbeat that confirms it until `confirmedUntil`. */
const keptLease = (claim: Claim, o: Partial<KeptLease> = {}): KeptLease => ({
  claim,
  signal: new AbortController().signal,
  lost: false,
  confirmedUntil: Infinity,
  abort() {},
  endHandler() {},
  release() {},
  ...o,
})

const failAction = { type: "fail", options: { error: "boom", backoff: 3600 } } as const

/** Settles a claim on a database over `driver`, and counts the errors that the settlement reports. */
async function run({
  driver,
  ...o
}: Partial<Omit<SettleOptions, "database">> & Pick<SettleOptions, "claim"> & { driver: Driver }) {
  const events: WorkerEvent[] = []
  const outcome = await settle({
    database: createDatabase({ driver }),
    table: schema.table,
    lease: keptLease(o.claim),
    action: { type: "complete" },
    retryDelay: 0.01,
    maxRetryDelay: 0.05,
    proceed: () => true,
    onEvent: e => events.push(e),
    ...o,
  })
  return { outcome, errors: events.filter(e => e.type === "settle-error").length }
}

for (const mode of ["before", "after"] as const) {
  test(`complete cut ${mode} it reaches the server: retried on a new connection, ${mode === "before" ? "completed" : "unknown"}`, async () => {
    const { proxy, driver } = await proxied()
    const claim = await claimJob()
    proxy.cutNext(/^\s*delete from \S+\.job\b/, mode)
    expect(await run({ driver, claim })).toEqual({
      outcome: mode === "before" ? "completed" : "unknown",
      errors: 1,
    })
    expect(await jobs(schema)).toEqual([])
  })
}

for (const mode of ["before", "after"] as const) {
  test(`fail cut ${mode} its COMMIT reaches the server: retried on a new connection, ${mode === "before" ? "applied" : "unknown"}`, async () => {
    const { proxy, driver } = await proxied()
    const claim = await claimJob()
    proxy.cutNext(/^\s*commit\b/i, mode)
    expect(await run({ driver, claim, action: failAction })).toEqual({
      outcome: mode === "before" ? "retry" : "unknown",
      errors: 1,
    })
    const row = only(await jobs(schema))
    expect([row.state, row.attempts, row.last_error]).toEqual(["pending", 1, "boom"])
  })
}

test("a claim that a heartbeat found lost is not settled", async () => {
  const claim = await claimJob()
  expect(
    await run({ driver: schema.driver, claim, lease: keptLease(claim, { lost: true }) }),
  ).toEqual({
    outcome: "lost",
    errors: 0,
  })
  expect(only(await jobs(schema)).state).toBe("active")
})

for (const unsent of [true, false]) {
  test(`once the worker may not proceed after a connection error, no attempt follows: ${unsent ? "stopped after an unsent statement" : "unknown after one of unknown outcome"}`, async () => {
    const { proxy, driver } = await proxied()
    const claim = await claimJob()
    if (unsent) proxy.refuse = true
    else proxy.cutNext(/^\s*delete from \S+\.job\b/, "after")
    const events: WorkerEvent[] = []
    const outcome = await settle({
      database: createDatabase({ driver }),
      table: schema.table,
      claim,
      lease: keptLease(claim),
      // The transaction of fail reserves its connection first, which a refused connection leaves unsent.
      action: unsent ? failAction : { type: "complete" },
      retryDelay: 0.01,
      maxRetryDelay: 0.05,
      proceed: () => events.length === 0,
      onEvent: e => events.push(e),
    })
    expect([outcome, events.map(e => e.type)]).toEqual([
      unsent ? "stopped" : "unknown",
      ["settle-error"],
    ])
    expect((await jobs(schema)).map(row => row.state)).toEqual(unsent ? ["active"] : [])
  })
}

test("a statement that PostgreSQL keeps rejecting is given up after three retries", async () => {
  const claim = await claimJob()
  const rejecting: Driver = {
    ...schema.driver,
    execute: async statement => {
      if (/^\s*delete from \S+\.job\b/.test(statement.text))
        throw new QueryError("canceling statement due to statement timeout", { code: "57014" })
      return schema.driver.execute(statement)
    },
  }
  expect(await run({ driver: rejecting, claim })).toEqual({ outcome: "error", errors: 4 })
  expect(only(await jobs(schema)).state).toBe("active")
})

test("rejections after a statement of unknown outcome give up the settlement with the outcome unknown", async () => {
  const claim = await claimJob()
  let deletes = 0
  const driver: Driver = {
    ...schema.driver,
    execute: async statement => {
      if (!/^\s*delete from \S+\.job\b/.test(statement.text))
        return schema.driver.execute(statement)
      if (deletes++ === 0) throw new ConnectionError("connection reset")
      throw new QueryError("canceling statement due to statement timeout", { code: "57014" })
    },
  }
  expect(await run({ driver, claim })).toEqual({ outcome: "unknown", errors: 5 })
})

test("connection errors past the confirmed lease give up the settlement: abandoned, and the job stays active", async () => {
  const { proxy, driver } = await proxied()
  const claim = await claimJob()
  proxy.refuse = true
  const started = monotonicNow()
  const result = await run({
    driver,
    claim,
    action: failAction,
    lease: keptLease(claim, { confirmedUntil: started + 200 }),
  })
  expect(result.outcome).toBe("abandoned")
  expect(result.errors).toBeGreaterThan(1)
  expect(monotonicNow() - started).toBeGreaterThanOrEqual(200)
  expect(only(await jobs(schema)).state).toBe("active")
})
