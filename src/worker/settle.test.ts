// Settlement by the worker: retries after connection loss with known and unknown outcomes, a lost claim, rejected statements, and giving up past the confirmed lease.

import { afterEach, beforeEach, expect, test } from "bun:test"

import { ConnectionError, type Driver, QueryError } from "kern-orm/db"

import { enqueue } from "../sql/enqueue"
import { toStatementLabel } from "../sql/statement"
import {
  createTestDriver,
  createTestSchema,
  databaseUrl,
  jobs,
  only,
  type TestSchema,
} from "../test-harness/database"

import { type Claim, claimPlain } from "./db/claim"
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
  await enqueue(schema.conn, schema.table, { task: "t" })
  return only(
    await claimPlain(schema.conn.query, {
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
}: Partial<Omit<SettleOptions, "db">> & Pick<SettleOptions, "claim"> & { driver: Driver }) {
  const events: WorkerEvent[] = []
  const outcome = await settle({
    db: schema.createDb(driver),
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
    proxy.cutNext(new RegExp(toStatementLabel("settle:complete")), mode)
    expect(await run({ driver, claim })).toEqual({
      outcome: mode === "before" ? "completed" : "unknown",
      errors: 1,
    })
    expect(await jobs(schema.conn.query)).toEqual([])
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
    const row = only(await jobs(schema.conn.query))
    expect([row.state, row.attempts, row.lastError]).toEqual(["pending", 1, "boom"])
  })
}

for (const mode of ["before", "after"] as const) {
  test(`terminal fail of a claim stamped with a failure job cut ${mode} its COMMIT reaches the server: one failure job either way`, async () => {
    const { proxy, driver } = await proxied()
    await enqueue(schema.conn, schema.table, { task: "t", maxAttempts: 1 })
    const claim = only(
      await claimPlain(schema.conn.query, {
        worker: "w",
        tasks: ["t"],
        limit: 1,
        defaultLease: 60,
        failureMaxAttempts: { t: 3 },
      }),
    )
    proxy.cutNext(/^\s*commit\b/i, mode)
    expect(await run({ driver, claim, action: failAction })).toEqual({
      outcome: mode === "before" ? "failed" : "unknown",
      errors: 1,
    })
    expect((await jobs(schema.conn.query)).map(r => [r.task, r.state])).toEqual([
      ["t", "failed"],
      ["t:failure", "pending"],
    ])
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
  expect(only(await jobs(schema.conn.query)).state).toBe("active")
})

for (const unsent of [true, false]) {
  test(`once the worker may not proceed after a connection error, no attempt follows: ${unsent ? "stopped after an unsent statement" : "unknown after one of unknown outcome"}`, async () => {
    const { proxy, driver } = await proxied()
    const claim = await claimJob()
    if (unsent) proxy.refuse = true
    else proxy.cutNext(new RegExp(toStatementLabel("settle:complete")), "after")
    const events: WorkerEvent[] = []
    const outcome = await settle({
      db: schema.createDb(driver),
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
    expect((await jobs(schema.conn.query)).map(row => row.state)).toEqual(unsent ? ["active"] : [])
  })
}

test("a statement that PostgreSQL keeps rejecting is given up after three retries", async () => {
  const claim = await claimJob()
  const rejecting: Driver = {
    ...schema.driver,
    execute: async statement => {
      if (statement.text.includes(toStatementLabel("settle:complete")))
        throw new QueryError("canceling statement due to statement timeout", { code: "57014" })
      return schema.driver.execute(statement)
    },
  }
  expect(await run({ driver: rejecting, claim })).toEqual({ outcome: "error", errors: 4 })
  expect(only(await jobs(schema.conn.query)).state).toBe("active")
})

test("rejections after a statement of unknown outcome give up the settlement with the outcome unknown", async () => {
  const claim = await claimJob()
  let deletes = 0
  const driver: Driver = {
    ...schema.driver,
    execute: async statement => {
      if (!statement.text.includes(toStatementLabel("settle:complete")))
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
  expect(only(await jobs(schema.conn.query)).state).toBe("active")
})
