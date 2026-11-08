// Measurements of the worker runtime: throughput of no-op handlers by concurrency against a bare claim-and-complete loop of the SQL core, and the cost of a heartbeat by the number of its claims against a plain UPDATE that waits for locks.
// Run: bun --env-file=.env.test.local src/worker/worker.bench.ts
import { claimCycle, claimPlain, createClaimState } from "../core/claim"
import { heartbeat } from "../core/lease"
import { complete } from "../core/settlement"
import { query } from "../core/statement"
import { createTestSchema, type TestSchema } from "../core/testing"

import { startRuntime } from "./runtime"

const JOBS = 3000

async function fill(schema: TestSchema, n: number) {
  await query(
    schema.admin,
    `insert into ${schema.table.name} (task, payload, run_at, window_started_at, created_at, max_attempts)
     select 't', '{}', now(), now(), now(), 25 from generate_series(1, $1::text::int)`,
    [n],
  )
}

async function runtimeThroughput(concurrency: number): Promise<number> {
  const schema = await createTestSchema()
  await fill(schema, JOBS)
  const driver = schema.createDriver({ max: concurrency + 3 })
  let settled = 0
  const done = Promise.withResolvers<void>()
  const started = performance.now()
  const worker = startRuntime({
    driver,
    table: schema.table,
    heartbeatSource: schema.createDriver({ max: 1 }),
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
  await schema.dispose()
  return (JOBS / ms) * 1000
}

async function bareThroughput(concurrency: number): Promise<number> {
  const schema = await createTestSchema()
  await fill(schema, JOBS)
  const driver = schema.createDriver({ max: concurrency + 1 })
  const state = createClaimState()
  let left = JOBS
  const started = performance.now()
  while (left > 0) {
    const claims = await claimCycle(
      driver,
      schema.table,
      { worker: "w", tasks: ["t"], slots: concurrency },
      undefined,
      state,
    )
    await Promise.all(claims.map(c => complete(driver, schema.table, c)))
    left -= claims.length
  }
  const ms = performance.now() - started
  await schema.dispose()
  return (JOBS / ms) * 1000
}

async function heartbeatCost(claimsCount: number) {
  const schema = await createTestSchema()
  await fill(schema, claimsCount)
  const claims = await claimPlain(schema.admin, schema.table, {
    worker: "w",
    tasks: ["t"],
    limit: claimsCount,
    defaultLease: 600,
  })
  const refs = claims.map(c => ({ id: c.id, claimId: c.claimId, lease: 60 }))
  const rounds = 50
  const time = async (fn: () => Promise<unknown>) => {
    await fn()
    const t = performance.now()
    for (let i = 0; i < rounds; i++) await fn()
    return (performance.now() - t) / rounds
  }
  const skipping = await time(() => heartbeat(schema.admin, schema.table, { claims: refs }))
  const plain = await time(() =>
    query(
      schema.admin,
      `update ${schema.table.name} j set lease_until = greatest(j.lease_until, now() + make_interval(secs => x.lease))
       from unnest($1::text::bigint[], $2::text::bigint[], $3::text::float8[]) as x(id, claim_id, lease)
       where j.id = x.id and j.claim_id = x.claim_id and j.state = 'active'
       returning j.claim_id`,
      [refs.map(r => r.id), refs.map(r => r.claimId), refs.map(r => String(r.lease))],
    ),
  )
  await schema.dispose()
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
