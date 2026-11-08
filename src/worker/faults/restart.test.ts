// PostgreSQL restart and crash recovery under running worker processes and a reaper process, on a private temporary cluster.
import { afterAll, beforeAll, describe, expect, test } from "bun:test"
import { existsSync, mkdtempSync, readFileSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"

import type { Driver } from "kern-db"

import { claimPlain } from "../../core/claim"
import { reap } from "../../core/lease"
import type { JobTable } from "../../core/schema"
import { query } from "../../core/statement"
import { createTestSchema, sleep, type TestSchema } from "../../core/testing"

import type { WorkerConfig } from "./worker"

// Binaries of the temporary cluster: PG_BIN, or the directory that pg_config reports.
const BIN = process.env.PG_BIN ?? Bun.spawnSync(["pg_config", "--bindir"]).stdout.toString().trim()
const PORT = 54329
const dir = mkdtempSync(join(tmpdir(), "kern-jobs-restart-"))
const data = join(dir, "data")
const URL = `postgres://postgres@127.0.0.1:${PORT}/kern_jobs`

const run = (cmd: string[]) => {
  const r = Bun.spawnSync(cmd, { stdout: "pipe", stderr: "pipe" })
  if (r.exitCode !== 0) throw new Error(`${cmd.join(" ")}: ${r.stderr.toString()}`)
}
const pgCtl = (...args: string[]) =>
  run([`${BIN}/pg_ctl`, "-D", data, "-l", join(dir, "server.log"), "-w", ...args])

beforeAll(() => {
  run([`${BIN}/initdb`, "-D", data, "-A", "trust", "-U", "postgres"])
  pgCtl("-o", `-p ${PORT} -k ${dir} -c listen_addresses=127.0.0.1`, "start")
  run([`${BIN}/createdb`, "-h", "127.0.0.1", "-p", String(PORT), "-U", "postgres", "kern_jobs"])
})
afterAll(() => {
  pgCtl("-m", "immediate", "stop")
})

interface Event {
  t: number
  w: string
  ev: string
  [k: string]: any
}

/** Counts the jobs left, retrying while the server is unavailable; the pool reconnects after the restart. */
async function countJobs(driver: Driver, table: JobTable): Promise<number> {
  for (;;) {
    try {
      return Number((await query(driver, `select count(*)::int as n from ${table.name}`))[0]!.n)
    } catch {
      await sleep(100)
    }
  }
}

/** Runs `fn` until it succeeds, while the server is unavailable. */
async function retrying<T>(fn: () => Promise<T>, timeoutMs = 10_000): Promise<T> {
  const deadline = Date.now() + timeoutMs
  for (;;) {
    try {
      return await fn()
    } catch (e) {
      if (Date.now() > deadline) throw e
      await sleep(100)
    }
  }
}

const readEvents = (log: string): Event[] =>
  existsSync(log)
    ? readFileSync(log, "utf8")
        .trim()
        .split("\n")
        .filter(Boolean)
        .map(x => JSON.parse(x))
    : []

describe("reaper after a restart", () => {
  test("pg_ctl restart -m fast: the reaper reconnects and returns jobs that expire after the restart", async () => {
    const schema: TestSchema = await createTestSchema(URL)
    const log = join(dir, "reaper.jsonl")
    const cfg: WorkerConfig = {
      id: "reaper",
      role: "reaper",
      schemaName: schema.name,
      mainUrl: URL,
      heartbeatUrl: URL,
      log,
      tasks: ["t"],
      slots: 0,
      lease: 1,
      heartbeatMs: 300,
      marginMs: 300,
      handlerMs: 0,
      honorAbort: true,
      pollMs: 100,
    }
    const proc = Bun.spawn(["bun", join(import.meta.dir, "worker.ts")], {
      env: { ...process.env, FAULT_CONFIG: JSON.stringify(cfg) },
      stdout: "inherit",
      stderr: "inherit",
    })
    try {
      const deadline = Date.now() + 10_000
      while (!readEvents(log).some(e => e.ev === "ready") && Date.now() < deadline) await sleep(50)
      const restartedAt = Date.now()
      pgCtl("-m", "fast", "restart")
      // Jobs of a dead worker whose leases expire after the restart.
      await retrying(() =>
        query(
          schema.driver,
          `insert into ${schema.table.name} (task, payload, run_at, window_started_at, created_at, max_attempts)
           select 't', '{}', now(), now(), now(), 25 from generate_series(1, 5)`,
        ),
      )
      const claims = await claimPlain(schema.driver, schema.table, {
        worker: "dead",
        tasks: ["t"],
        limit: 5,
        defaultLease: 0.2,
      })
      expect(claims).toHaveLength(5)
      const until = Date.now() + 10_000
      let pending = 0
      while (Date.now() < until) {
        pending = Number(
          (
            await query(
              schema.driver,
              `select count(*) as n from ${schema.table.name} where state = 'pending'`,
            )
          )[0]!.n,
        )
        if (pending === 5) break
        await sleep(100)
      }
      const reapedAt = readEvents(log).findLast(e => e.ev === "reap")?.t
      console.log(
        `reaper after restart -m fast: ${pending} of 5 returned, ` +
          `${reapedAt ? `${reapedAt - restartedAt} ms after the restart` : "never"}, process alive: ${proc.exitCode == null}`,
      )
      expect(proc.exitCode).toBeNull()
      expect(pending).toBe(5)
    } finally {
      proc.kill(9)
      await schema.dispose()
    }
  }, 30_000)
})

describe("PostgreSQL restart under load", () => {
  for (const mode of ["fast", "immediate"] as const) {
    test(`pg_ctl restart -m ${mode}: every job completes, workers recover`, async () => {
      const schema: TestSchema = await createTestSchema(URL)
      await query(
        schema.admin,
        `insert into ${schema.table.name} (task, queue, key, payload, run_at, window_started_at, created_at, max_attempts)
         select 't', case when i % 3 = 0 then 'q' || (i % 5) end, case when i % 3 = 1 then 'k' || i end,
                to_jsonb(i), now(), now(), now(), 25
         from generate_series(1, 300) i`,
      )
      const ids = new Set(
        (await query(schema.admin, `select id from ${schema.table.name}`)).map(r => r.id),
      )
      const observer = schema.createDriver({ max: 2 })

      const logs = ["a", "b", "c"].map(id => join(dir, `${mode}-${id}.jsonl`))
      const procs = ["a", "b", "c"].map((id, i) => {
        const cfg: WorkerConfig = {
          id,
          role: "worker",
          schemaName: schema.name,
          mainUrl: URL,
          heartbeatUrl: URL,
          log: logs[i]!,
          tasks: ["t"],
          slots: 2,
          lease: 2,
          heartbeatMs: 300,
          marginMs: 500,
          handlerMs: 30,
          honorAbort: true,
          pollMs: 10,
        }
        return Bun.spawn(["bun", join(import.meta.dir, "worker.ts")], {
          env: { ...process.env, FAULT_CONFIG: JSON.stringify(cfg) },
          stdout: "inherit",
          stderr: "inherit",
        })
      })
      let reaping = true
      const reaper = (async () => {
        while (reaping) {
          try {
            await reap(observer, schema.table)
          } catch {}
          await sleep(200)
        }
      })()

      await sleep(800)
      const restartedAt = Date.now()
      pgCtl("-m", mode, "restart")
      const downMs = Date.now() - restartedAt

      const deadline = Date.now() + 60_000
      while ((await countJobs(observer, schema.table)) > 0 && Date.now() < deadline)
        await sleep(200)
      const left = await countJobs(observer, schema.table)
      reaping = false
      await reaper
      const alive = procs.every(p => p.exitCode == null)
      for (const p of procs) p.kill(9)

      const events: Event[] = logs.flatMap(readEvents)
      const outcomes = (outcome: string) =>
        events.filter(e => e.ev === "settled" && e.outcome === outcome).map(e => e.id)
      const completed = outcomes("completed")
      const unknown = outcomes("unknown")
      const starts = new Map<string, number>()
      for (const e of events) if (e.ev === "start") starts.set(e.id, (starts.get(e.id) ?? 0) + 1)
      const duplicates = [...starts.values()].filter(n => n > 1).length
      const errors = (ev: string) => events.filter(e => e.ev === ev).length
      const recoveredAt = Math.min(
        ...events.filter(e => e.t > restartedAt + downMs && e.ev === "start").map(e => e.t),
      )
      console.log(
        `restart -m ${mode}: server down ${downMs} ms, first start after restart +${recoveredAt - restartedAt} ms, ` +
          `${completed.length} completions and ${unknown.length} of unknown outcome, ` +
          `${duplicates} jobs started more than once, errors: ${errors("claim-error")} claim, ` +
          `${errors("settle-error")} settlement, ${errors("heartbeat-error")} heartbeat; ` +
          `${outcomes("fenced").length + outcomes("lost").length} fenced or lost`,
      )
      expect(left).toBe(0)
      expect(alive).toBe(true)
      // Every job completed, and none twice: a completion of unknown outcome may be the one that applied.
      expect(new Set([...completed, ...unknown])).toEqual(ids)
      expect(new Set(completed).size).toBe(completed.length)
      expect([...outcomes("error"), ...outcomes("abandoned")]).toEqual([])
      await schema.dispose()
    }, 90_000)
  }
})
