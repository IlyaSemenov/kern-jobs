// Measurements of the SQL core: hot-key upsert churn, claim latency under backlog, idle cycle cost, reaper and fail latency.
// Run: bun --env-file=.env.test.local src/core/core.bench.ts
import { claimCycle, claimPlain, claimQueued } from "./claim"
import { enqueue } from "./enqueue"
import type { KeyMode } from "./job"
import { reap } from "./lease"
import { complete, fail } from "./settlement"
import { query } from "./statement"
import { createTestSchema, type TestSchema } from "./testing"

const percentile = (xs: number[], p: number) =>
  [...xs].sort((a, b) => a - b)[Math.min(xs.length - 1, Math.floor(xs.length * p))]!
const fmt = (xs: number[]) =>
  `avg ${(xs.reduce((a, b) => a + b, 0) / xs.length).toFixed(2)} ms, p95 ${percentile(xs, 0.95).toFixed(2)} ms`

async function time<T>(fn: () => Promise<T>): Promise<[number, T]> {
  const t = performance.now()
  const r = await fn()
  return [performance.now() - t, r]
}

async function sizes(schema: TestSchema) {
  await query(schema.admin, `analyze ${schema.table.name}`)
  const [r] = await query(
    schema.admin,
    `select pg_relation_size($1::text::regclass) as heap, pg_indexes_size($1::text::regclass) as idx,
            (select n_dead_tup from pg_stat_user_tables where relid = $1::text::regclass) as dead,
            (select n_live_tup from pg_stat_user_tables where relid = $1::text::regclass) as live`,
    [schema.table.name],
  )
  return `heap ${(Number(r!.heap) / 1024).toFixed(0)} KiB, indexes ${(Number(r!.idx) / 1024).toFixed(0)} KiB, live ${r!.live}, dead ${r!.dead}`
}

async function hotKey(mode: KeyMode) {
  const schema = await createTestSchema()
  const conns = await Promise.all(Array.from({ length: 8 }, () => schema.connect()))
  const total = 20_000
  const [ms] = await time(() =>
    Promise.all(
      conns.map(async c => {
        for (let i = 0; i < total / conns.length; i++)
          await enqueue(c, schema.table, {
            task: "t",
            key: "hot",
            mode,
            payload: { i },
            runAt: new Date(Date.now() + 60_000),
            delay: 60,
            maxWait: 600,
          })
      }),
    ),
  )
  console.log(
    `hot key ${mode}: ${total} upserts from 8 connections in ${ms.toFixed(0)} ms (${((total / ms) * 1000).toFixed(0)}/s)`,
  )
  console.log(`  before vacuum: ${await sizes(schema)}`)
  await query(schema.admin, `vacuum ${schema.table.name}`)
  console.log(`  after vacuum:  ${await sizes(schema)}`)
  await schema.dispose()
}

async function claimBacklog() {
  const schema = await createTestSchema()
  await query(
    schema.admin,
    `insert into ${schema.table.name} (task, payload, run_at, window_started_at, created_at, max_attempts)
     select 't', '{}', now() + interval '1 day', now(), now(), 25 from generate_series(1, 100000)`,
  )
  await query(
    schema.admin,
    `insert into ${schema.table.name} (task, payload, run_at, window_started_at, created_at, max_attempts)
     select 't', '{}', now() - interval '1 second', now(), now(), 25 from generate_series(1, 10000)`,
  )
  await query(schema.admin, `analyze ${schema.table.name}`)
  const lat: number[] = []
  for (let i = 0; i < 200; i++) {
    const [ms, claims] = await time(() =>
      claimPlain(schema.admin, schema.table, { worker: "w", tasks: ["t"], limit: 4 }),
    )
    lat.push(ms)
    for (const c of claims) await complete(schema.admin, schema.table, c)
  }
  console.log(`claimPlain, 100k future + 10k ready plain rows, limit 4: ${fmt(lat)}`)
  await schema.dispose()
}

async function queueBacklog(queues: number) {
  const schema = await createTestSchema()
  await query(
    schema.admin,
    `insert into ${schema.table.name} (task, queue, payload, run_at, window_started_at, created_at, max_attempts)
     select 't', 'q' || (i % $1), '{}', now() - interval '1 second', now(), now(), 25 from generate_series(1, 100000) i`,
    [queues],
  )
  await query(schema.admin, `analyze ${schema.table.name}`)
  const measure = async (label: string) => {
    const lat: number[] = []
    for (let i = 0; i < 100; i++) {
      const [ms, r] = await time(() =>
        claimQueued(schema.admin, schema.table, { worker: "w", tasks: ["t"] }),
      )
      lat.push(ms)
      if (r.kind === "job") await complete(schema.admin, schema.table, r.claim)
    }
    console.log(`claimQueued, 100k ready rows in ${queues} queues, ${label}: ${fmt(lat)}`)
  }
  await measure("all queues free")
  // A long job keeps the first queue busy; its backlog stays ready.
  const busy = await claimQueued(schema.admin, schema.table, { worker: "long", tasks: ["t"] })
  await measure("first queue busy")
  const cycles: number[] = []
  const conns = await Promise.all(Array.from({ length: 8 }, () => schema.connect()))
  let done = 0
  const [ms] = await time(() =>
    Promise.all(
      conns.map(async (c, wi) => {
        for (let i = 0; i < 25; i++) {
          const [t, claims] = await time(() =>
            claimCycle(c, schema.table, { worker: `w${wi}`, tasks: ["t"], slots: 4 }),
          )
          cycles.push(t)
          for (const cl of claims) if (await complete(c, schema.table, cl)) done++
        }
      }),
    ),
  )
  console.log(
    `  8 workers × 25 cycles × 4 slots, queue jobs only: ${done} jobs in ${ms.toFixed(0)} ms; cycle ${fmt(cycles)}`,
  )
  void busy
  await schema.dispose()
}

async function idleCycle() {
  const schema = await createTestSchema()
  const lat: number[] = []
  for (let i = 0; i < 500; i++) {
    const [ms] = await time(() =>
      claimCycle(schema.admin, schema.table, { worker: "w", tasks: ["t"], slots: 4 }),
    )
    lat.push(ms)
  }
  console.log(`idle claim cycle on empty table (2 statements): ${fmt(lat)}`)
  await schema.dispose()
}

async function reapAndFail() {
  const schema = await createTestSchema()
  await query(
    schema.admin,
    `insert into ${schema.table.name} (task, key, payload, run_at, window_started_at, created_at, max_attempts)
     select 't', 'k' || i, '{}', now(), now(), now(), 25 from generate_series(1, 1000) i`,
  )
  await claimPlain(schema.admin, schema.table, {
    worker: "dead",
    tasks: ["t"],
    limit: 1000,
    defaultLease: 0.001,
  })
  await new Promise(r => setTimeout(r, 10))
  const [ms, reaped] = await time(() => reap(schema.admin, schema.table, { limit: 1000 }))
  console.log(
    `reap 1000 expired rows in one statement: ${ms.toFixed(1)} ms (${reaped.length} reaped)`,
  )

  const claims = await claimPlain(schema.admin, schema.table, {
    worker: "w",
    tasks: ["t"],
    limit: 1000,
  })
  for (let i = 0; i < 500; i++)
    await enqueue(schema.admin, schema.table, { task: "t", key: `k${i + 1}` })
  const lat: number[] = []
  for (const c of claims) {
    const [t] = await time(() =>
      fail(schema.database, schema.table, c, { error: "e", backoff: 10 }),
    )
    lat.push(t)
  }
  console.log(
    `fail transition (4–5 statements in a transaction, half with competitor): ${fmt(lat)}`,
  )
  await schema.dispose()
}

for (const mode of ["replace", "keep", "debounce"] as KeyMode[]) await hotKey(mode)
await claimBacklog()
await queueBacklog(3)
await queueBacklog(1000)
await idleCycle()
await reapAndFail()
