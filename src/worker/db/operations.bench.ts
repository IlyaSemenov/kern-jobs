// Measurements of the SQL core: hot-key upsert churn, claim latency under backlog, idle cycle cost, reaper and fail latency, and the cost of failure jobs.
// Run: bun --env-file=.env.test.local src/worker/db/operations.bench.ts

import { param } from "kern-orm/db"

import { enqueue } from "../../sql/enqueue"
import type { KeyMode } from "../../sql/job"
import { createTestSchema, type TestSchema } from "../../test-harness/database"

import { claimCycle, claimPlain, claimQueued } from "./claim"
import { workerSchema } from "./db"
import { reap } from "./lease"
import { complete, fail } from "./settlement"
import * as t from "./types"

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
  await schema.conn.query`analyze ${workerSchema.job}`
  const table = param(t.text, schema.table.name)
  const r = await schema.conn.query`
    select pg_relation_size(${table}::regclass) as heap, pg_indexes_size(${table}::regclass) as idx,
           (select n_dead_tup from pg_stat_user_tables where relid = ${table}::regclass) as dead,
           (select n_live_tup from pg_stat_user_tables where relid = ${table}::regclass) as live`
    .select({ heap: t.bigintText, idx: t.bigintText, dead: t.bigintText, live: t.bigintText })
    .take()
  return `heap ${(Number(r.heap) / 1024).toFixed(0)} KiB, indexes ${(Number(r.idx) / 1024).toFixed(0)} KiB, live ${r.live}, dead ${r.dead}`
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
  await schema.conn.query`vacuum ${workerSchema.job}`
  console.log(`  after vacuum:  ${await sizes(schema)}`)
  await schema.dispose()
}

async function claimBacklog() {
  const schema = await createTestSchema()
  await schema.conn.query`
    insert into ${workerSchema.job} (task, payload, run_at, window_started_at, created_at, max_attempts)
    select 't', '{}', now() + interval '1 day', now(), now(), 25 from generate_series(1, 100000)`
  await schema.conn.query`
    insert into ${workerSchema.job} (task, payload, run_at, window_started_at, created_at, max_attempts)
    select 't', '{}', now() - interval '1 second', now(), now(), 25 from generate_series(1, 10000)`
  await schema.conn.query`analyze ${workerSchema.job}`
  const lat: number[] = []
  for (let i = 0; i < 200; i++) {
    const [ms, claims] = await time(() =>
      claimPlain(schema.conn.query, { worker: "w", tasks: ["t"], limit: 4 }),
    )
    lat.push(ms)
    for (const c of claims) await complete(schema.db, c)
  }
  console.log(`claimPlain, 100k future + 10k ready plain rows, limit 4: ${fmt(lat)}`)
  await schema.dispose()
}

async function queueBacklog(queues: number) {
  const schema = await createTestSchema()
  await schema.conn.query`
    insert into ${workerSchema.job} (task, queue, payload, run_at, window_started_at, created_at, max_attempts)
    select 't', 'q' || (i % ${queues}::int), '{}', now() - interval '1 second', now(), now(), 25 from generate_series(1, 100000) i`
  await schema.conn.query`analyze ${workerSchema.job}`
  const measure = async (label: string) => {
    const lat: number[] = []
    for (let i = 0; i < 100; i++) {
      const [ms, r] = await time(() =>
        claimQueued(schema.conn.query, { worker: "w", tasks: ["t"] }),
      )
      lat.push(ms)
      if (r.kind === "job") await complete(schema.db, r.claim)
    }
    console.log(`claimQueued, 100k ready rows in ${queues} queues, ${label}: ${fmt(lat)}`)
  }
  await measure("all queues free")
  // A long job keeps the first queue busy; its backlog stays ready.
  const busy = await claimQueued(schema.conn.query, { worker: "long", tasks: ["t"] })
  await measure("first queue busy")
  const cycles: number[] = []
  const workers = Array.from({ length: 8 }, () => schema.createDb({ max: 1 }))
  let done = 0
  const [ms] = await time(() =>
    Promise.all(
      workers.map(async (db, wi) => {
        for (let i = 0; i < 25; i++) {
          const [t, claims] = await time(() =>
            claimCycle(schema.db.$queryOn(db.$database), {
              worker: `w${wi}`,
              tasks: ["t"],
              slots: 4,
            }),
          )
          cycles.push(t)
          for (const cl of claims) if (await complete(db, cl)) done++
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
      claimCycle(schema.conn.query, { worker: "w", tasks: ["t"], slots: 4 }),
    )
    lat.push(ms)
  }
  console.log(`idle claim cycle on empty table (2 statements): ${fmt(lat)}`)
  await schema.dispose()
}

async function reapAndFail() {
  const schema = await createTestSchema()
  await schema.conn.query`
    insert into ${workerSchema.job} (task, key, payload, run_at, window_started_at, created_at, max_attempts)
    select 't', 'k' || i, '{}', now(), now(), now(), 25 from generate_series(1, 1000) i`
  await claimPlain(schema.conn.query, {
    worker: "dead",
    tasks: ["t"],
    limit: 1000,
    defaultLease: 0.001,
  })
  await new Promise(r => setTimeout(r, 10))
  const [ms, reaped] = await time(() => reap(schema.conn.query, { limit: 1000 }))
  console.log(
    `reap 1000 expired rows in one statement: ${ms.toFixed(1)} ms (${reaped.length} reaped)`,
  )

  const claims = await claimPlain(schema.conn.query, {
    worker: "w",
    tasks: ["t"],
    limit: 1000,
  })
  for (let i = 0; i < 500; i++)
    await enqueue(schema.conn, schema.table, { task: "t", key: `k${i + 1}` })
  const lat: number[] = []
  for (const c of claims) {
    const [t] = await time(() => fail(schema.db, c, { error: "e", backoff: 10 }))
    lat.push(t)
  }
  console.log(
    `fail transition (4–5 statements in a transaction, half with competitor): ${fmt(lat)}`,
  )
  await schema.dispose()
}

/** Claims, a reap, and terminal fails with and without stamps of failure jobs, alternating so that both see the same table. */
async function failureJobs() {
  const schema = await createTestSchema()
  const failureMaxAttempts = { t: 25 }
  await schema.conn.query`
    insert into ${workerSchema.job} (task, queue, payload, run_at, window_started_at, created_at, max_attempts)
    select 't', case when i % 2 = 0 then 'q' || ((i / 2) % 100) end, '{"id": 1}', now() - interval '1 second', now(), now(), 1
    from generate_series(1, 20000) i`
  await schema.conn.query`analyze ${workerSchema.job}`
  const plain = { without: [] as number[], with: [] as number[] }
  const queued = { without: [] as number[], with: [] as number[] }
  for (let i = 0; i < 400; i++) {
    const variant = i % 2 === 0 ? "without" : "with"
    const o = {
      worker: "w",
      tasks: ["t"],
      failureMaxAttempts: variant === "with" ? failureMaxAttempts : {},
    }
    const [p, claims] = await time(() => claimPlain(schema.conn.query, { ...o, limit: 4 }))
    plain[variant].push(p)
    const [q, r] = await time(() => claimQueued(schema.conn.query, o))
    queued[variant].push(q)
    for (const c of [...claims, ...(r.kind === "job" ? [r.claim] : [])])
      await complete(schema.db, c)
  }
  console.log(
    `claimPlain, limit 4, without stamps: ${fmt(plain.without)}; with: ${fmt(plain.with)}`,
  )
  console.log(
    `claimQueued, 100 queues, without stamps: ${fmt(queued.without)}; with: ${fmt(queued.with)}`,
  )
  for (const variant of ["without", "with"] as const) {
    const o = {
      worker: "dead",
      tasks: ["t"],
      failureMaxAttempts: variant === "with" ? failureMaxAttempts : {},
    }
    await claimPlain(schema.conn.query, { ...o, limit: 1000, defaultLease: 0.001 })
    await new Promise(r => setTimeout(r, 10))
    const [ms, reaped] = await time(() => reap(schema.conn.query, { limit: 1000 }))
    console.log(
      `reap 1000 expired rows on their last attempt ${variant} stamps: ${ms.toFixed(1)} ms (${reaped.length} failed)`,
    )
  }
  const fails = { without: [] as number[], with: [] as number[] }
  for (let i = 0; i < 400; i++) {
    const variant = i % 2 === 0 ? "without" : "with"
    const o = {
      worker: "w",
      tasks: ["t"],
      failureMaxAttempts: variant === "with" ? failureMaxAttempts : {},
    }
    const [c] = await claimPlain(schema.conn.query, { ...o, limit: 1 })
    const [t] = await time(() => fail(schema.db, c!, { error: "e", backoff: 10 }))
    fails[variant].push(t)
  }
  console.log(`terminal fail without stamps: ${fmt(fails.without)}; with: ${fmt(fails.with)}`)
  await schema.dispose()
}

for (const mode of ["replace", "keep", "debounce"] as KeyMode[]) await hotKey(mode)
await claimBacklog()
await queueBacklog(3)
await queueBacklog(1000)
await idleCycle()
await reapAndFail()
await failureJobs()
