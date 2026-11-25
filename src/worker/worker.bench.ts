// Measurements of the worker runtime: throughput of no-op handlers by concurrency against a bare claim-and-complete loop of the SQL core, and the cost of a heartbeat by the number of its claims against a plain UPDATE that waits for locks.
// Run: bun --env-file=.env.test.local src/worker/worker.bench.ts

import { param } from "kern-orm/db"

import { createTestSchema, type TestSchema } from "../test-harness/database"

import { claimCycle, claimPlain, createClaimState } from "./db/claim"
import { schema } from "./db/db"
import { heartbeat } from "./db/lease"
import { complete } from "./db/settlement"
import * as t from "./db/types"
import { startRuntime } from "./runtime"

const JOBS = 3000

async function fill(testSchema: TestSchema, n: number) {
  await testSchema.conn.query`
    insert into ${schema.job} (task, payload, run_at, window_started_at, created_at, max_attempts)
    select 't', '{}', now(), now(), now(), 25 from generate_series(1, ${n}::int)`
}

async function runtimeThroughput(concurrency: number): Promise<number> {
  const testSchema = await createTestSchema()
  await fill(testSchema, JOBS)
  const driver = testSchema.createDriver({ max: concurrency + 3 })
  let settled = 0
  const done = Promise.withResolvers<void>()
  const started = performance.now()
  const worker = startRuntime({
    driver,
    table: testSchema.table,
    heartbeatSource: testSchema.createDriver({ max: 1 }),
    tasks: { t: { handler() {} } },
    id: "bench",
    concurrency,
    lease: 60,
    pollInterval: 0.01,
    reaper: false,
    onEvent: e => {
      if (e.type === "settled" && ++settled === JOBS) done.resolve()
    },
  })
  await done.promise
  const ms = performance.now() - started
  await worker.stop()
  await testSchema.dispose()
  return (JOBS / ms) * 1000
}

async function bareThroughput(concurrency: number): Promise<number> {
  const testSchema = await createTestSchema()
  await fill(testSchema, JOBS)
  const driver = testSchema.createDriver({ max: concurrency + 1 })
  const db = testSchema.createDb(driver)
  const state = createClaimState()
  let left = JOBS
  const started = performance.now()
  while (left > 0) {
    const claims = await claimCycle(
      testSchema.db.$queryOn(driver),
      { worker: "w", tasks: ["t"], slots: concurrency },
      undefined,
      state,
    )
    await Promise.all(claims.map(c => complete(db, c)))
    left -= claims.length
  }
  const ms = performance.now() - started
  await testSchema.dispose()
  return (JOBS / ms) * 1000
}

async function heartbeatCost(claimsCount: number) {
  const testSchema = await createTestSchema()
  await fill(testSchema, claimsCount)
  const claims = await claimPlain(testSchema.conn.query, {
    worker: "w",
    tasks: ["t"],
    limit: claimsCount,
    defaultLease: 600,
  })
  const refs = claims.map(c => ({ id: c.id, claimId: c.claimId, lease: 60 }))
  const rounds = 50
  const time = async (fn: () => PromiseLike<unknown>) => {
    await fn()
    const t = performance.now()
    for (let i = 0; i < rounds; i++) await fn()
    return (performance.now() - t) / rounds
  }
  const skipping = await time(() => heartbeat(testSchema.conn.query, { claims: refs }))
  const plain = await time(() =>
    testSchema.conn.query`
        update ${schema.job} j set lease_until = greatest(j.lease_until, now() + make_interval(secs => x.lease))
        from unnest(${param(
          t.array(t.bigintText),
          refs.map(r => r.id),
        )}, ${param(
          t.array(t.bigintText),
          refs.map(r => r.claimId),
        )}, ${param(
          t.array(t.doublePrecision),
          refs.map(r => r.lease),
        )}) as x(id, claim_id, lease)
        where j.id = x.id and j.claim_id = x.claim_id and j.state = 'active'
        returning j.claim_id`.pluck(t.bigintText),
  )
  await testSchema.dispose()
  return { skipping, plain }
}

for (const concurrency of [1, 8, 32]) {
  const runtime = await runtimeThroughput(concurrency)
  const bare = await bareThroughput(concurrency)
  console.log(
    `concurrency ${concurrency}: runtime ${runtime.toFixed(0)} jobs/s, bare claim-and-complete loop ${bare.toFixed(0)} jobs/s, ratio ${(runtime / bare).toFixed(2)}`,
  )
}
for (const n of [1, 10, 100, 1000]) {
  const { skipping, plain } = await heartbeatCost(n)
  console.log(
    `heartbeat of ${n} claims: skipping locked rows ${skipping.toFixed(2)} ms, plain update ${plain.toFixed(2)} ms, ratio ${(skipping / plain).toFixed(2)}`,
  )
}
