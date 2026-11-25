// Failure jobs: the attempt budget that each claim stamps, the failure job that a terminal fail and the reaper insert with the JobFailure of the stored row, the outcomes that insert none, and the atomicity and uniqueness of the insert.

import { afterEach, beforeEach, describe, expect, test } from "bun:test"

import { literal, QueryError, queryOn, sql } from "kern-orm/db"
import { waitForLock } from "kern-orm/testing"

import { retryFailedJob } from "../../producer/administration"
import { enqueue } from "../../sql/enqueue"
import type { KeyMode } from "../../sql/job"
import {
  createTestSchema,
  holdHook,
  type JobRow,
  jobs,
  only,
  T,
  type TestSchema,
  unsafeInteger,
} from "../../test-harness/database"

import { type Claim, claimCycle, type ClaimOptions, claimPlain } from "./claim"
import { workerSchema } from "./db"
import { reap } from "./lease"
import { fail, requeue } from "./settlement"

let schema: TestSchema
beforeEach(async () => {
  schema = await createTestSchema()
})
afterEach(async () => {
  await schema.dispose()
})

/** Claim options of a worker of task `t` whose claims stamp failure jobs of 7 attempts. */
const stamping: ClaimOptions = {
  worker: "w",
  tasks: ["t"],
  defaultLease: 10,
  failureMaxAttempts: { t: 7 },
}

/** Enqueues a job of task `t` at T(0) and claims it at T(0) with the claim options `o`. */
async function claimed(
  enqueueOptions: Partial<Parameters<typeof enqueue>[2]> = {},
  o: ClaimOptions = stamping,
): Promise<Claim> {
  await enqueue(schema.conn, schema.table, { task: "t", now: T(0), ...enqueueOptions })
  return only(await claimCycle(schema.conn.query, { ...o, slots: 1, now: T(0) }))
}

const failAt = (claim: Claim, o: Partial<Parameters<typeof fail>[2]> = {}) =>
  fail(schema.db, claim, { error: "boom", backoff: 30, now: T(2), ...o })

/** Returns the failure jobs of task `t`. */
const failureJobs = (): Promise<JobRow[]> => jobs(schema.conn.query, sql`task = 't:failure'`)

/** Returns the JobFailure of the only failure job. */
async function jobFailure(): Promise<Record<string, unknown>> {
  return only(await failureJobs()).payload as Record<string, unknown>
}

describe("claim stamps", () => {
  for (const queue of [null, "q"]) {
    test(`a claim ${queue ? "with" : "without"} a queue stamps the attempt budget of the failure jobs of its task, and a later claim without one clears it`, async () => {
      const claim = await claimed({ queue, maxAttempts: 5 })
      expect(only(await jobs(schema.conn.query)).failureMaxAttempts).toBe(7)
      // A requeue keeps the stamp on the pending row, where nothing reads it.
      await requeue(schema.db, claim, { now: T(1) })
      const plain = { ...stamping, failureMaxAttempts: {} }
      await claimCycle(schema.conn.query, { ...plain, slots: 1, now: T(2) })
      const cleared = only(await jobs(schema.conn.query))
      expect([cleared.state, cleared.failureMaxAttempts]).toEqual(["active", null])
    })
  }

  test("a claim stamps the budget of the task of each job, and none for a task without one", async () => {
    await enqueue(schema.conn, schema.table, { task: "t", now: T(0) })
    await enqueue(schema.conn, schema.table, { task: "u", now: T(0) })
    await claimPlain(schema.conn.query, {
      ...stamping,
      tasks: ["t", "u"],
      limit: 2,
      now: T(0),
    })
    expect((await jobs(schema.conn.query)).map(r => [r.task, r.failureMaxAttempts])).toEqual([
      ["t", 7],
      ["u", null],
    ])
  })

  test("the final claim decides: a job claimed with a stamp and then without one inserts no failure job", async () => {
    const first = await claimed({ maxAttempts: 2 })
    expect(await failAt(first)).toBe("retry")
    const last = only(
      await claimPlain(schema.conn.query, {
        ...stamping,
        failureMaxAttempts: {},
        limit: 1,
        now: T(40),
      }),
    )
    expect(await failAt(last, { now: T(41) })).toBe("failed")
    expect(await failureJobs()).toEqual([])
  })

  test("a claim rejects a stamp for a task whose name is longer than 120 bytes, which leaves no room for the name of its failure task, and a budget below 1", async () => {
    // "é" takes 2 bytes.
    const fitting = "é".repeat(60)
    const long = `${fitting}x`
    for (const task of [fitting, long, "t"]) {
      await enqueue(schema.conn, schema.table, { task, now: T(0) })
    }
    const claim = (task: string, budget: number | undefined) =>
      claimPlain(schema.conn.query, {
        worker: "w",
        tasks: [task],
        failureMaxAttempts: budget === undefined ? {} : { [task]: budget },
        limit: 1,
        now: T(0),
      })
    for (const [task, budget] of [
      [long, 7],
      ["t", 0],
    ] as const) {
      await expect(claim(task, budget)).rejects.toMatchObject({
        constraint: "job_failure_max_attempts_check",
      })
    }
    expect(await claim(long, undefined)).toHaveLength(1)
    const stamped = only(await claim(fitting, 7))
    expect(await failAt(stamped, { reason: "terminal-error" })).toBe("failed")
    expect(
      only(await jobs(schema.conn.query, sql`task = ${`${fitting}:failure`}`)).maxAttempts,
    ).toBe(7)
  })
})

describe("terminal fail", () => {
  test("inserts one pending failure job of the failure task of the task and of the stamped budget, of the scope of the job, due at once, without a key or a queue, with the JobFailure of the stored row", async () => {
    const claim = await claimed({
      scope: "s1",
      key: "k",
      queue: "q",
      payload: { a: 1, nested: { n: null } },
      maxAttempts: 1,
    })
    expect(await failAt(claim)).toBe("failed")
    const source = only(await jobs(schema.conn.query, sql`task = 't'`))
    expect([source.state, source.key, source.lastError]).toEqual(["failed", "k", "boom"])
    const job = only(await failureJobs())
    expect(job).toMatchObject({
      task: "t:failure",
      scope: "s1",
      key: null,
      queue: null,
      keyMode: "replace",
      runAt: T(2),
      notBefore: null,
      windowStartedAt: T(2),
      createdAt: T(2),
      attempts: 0,
      maxAttempts: 7,
      state: "pending",
      claimId: null,
      lastError: null,
      failedAt: null,
      failureMaxAttempts: null,
    })
    expect(job.payload).toEqual({
      id: claim.claimId,
      reason: "handler-error",
      error: { message: "boom" },
      failedAt: "2030-01-01T00:00:02.000Z",
      job: {
        id: claim.id,
        task: "t",
        scope: "s1",
        attempt: 1,
        maxAttempts: 1,
        key: "k",
        queue: "q",
      },
      payload: { a: 1, nested: { n: null } },
    })
  })

  test("records the reason of the fail", async () => {
    const cases = [
      // A TerminalError fails the job before its last attempt.
      { maxAttempts: 5, reason: "terminal-error" },
      { maxAttempts: 1, reason: "late-claim" },
      { maxAttempts: 1, reason: "handler-error" },
    ] as const
    for (const { maxAttempts, reason } of cases) {
      await schema.conn.query`delete from ${workerSchema.job}`
      const claim = await claimed({ maxAttempts })
      expect(await failAt(claim, { reason })).toBe("failed")
      expect((await jobFailure()).reason).toBe(reason)
    }
  })

  test("a job without a scope gives a null scope in the JobFailure and an unscoped failure job", async () => {
    const claim = await claimed({ maxAttempts: 1 })
    await failAt(claim)
    const job = only(await failureJobs())
    expect(job.scope).toBe("")
    expect((job.payload as { job: { scope: unknown } }).job.scope).toBeNull()
  })

  test("the JobFailure omits the payload of a job enqueued without one, and keeps a JSON null", async () => {
    for (const payload of [undefined, null, "text"]) {
      await schema.conn.query`delete from ${workerSchema.job}`
      await failAt(await claimed({ payload, maxAttempts: 1 }))
      const e = await jobFailure()
      if (payload === undefined) expect("payload" in e).toBe(false)
      else expect(e.payload).toBe(payload)
    }
  })

  test("ids of the JobFailure are text, also above the safe integers of JavaScript", async () => {
    const id = unsafeInteger
    await schema.conn.query`
      alter table ${workerSchema.job} alter column id restart with ${literal(id)}`
    await schema.conn.query`select ${workerSchema.claimSeq.setval(id)}`
    const claim = await claimed({ maxAttempts: 1 })
    await failAt(claim)
    const e = (await jobFailure()) as { id: string; job: { id: string } }
    expect([e.id, e.job.id]).toEqual([String(id + 1n), String(id)])
    expect([claim.claimId, claim.id]).toEqual([String(id + 1n), String(id)])
  })

  test("failedAt is in UTC with milliseconds whatever the TimeZone and DateStyle of the session", async () => {
    const claim = await claimed({ maxAttempts: 1 })
    await failAt(claim, {
      now: T(2.5),
      pause: {
        afterOwnLock: async tx => {
          await queryOn(tx)`set local timezone = 'Asia/Tokyo'`
          await queryOn(tx)`set local datestyle = 'SQL, DMY'`
        },
      },
    })
    expect((await jobFailure()).failedAt).toBe("2030-01-01T00:00:02.500Z")
  })

  test("a terminal fail with a pending job of its key fails the job, inserts the failure job, and leaves the pending job", async () => {
    const claim = await claimed({ key: "k", maxAttempts: 1 })
    const pending = await enqueue(schema.conn, schema.table, { task: "t", key: "k", now: T(1) })
    expect(await failAt(claim)).toBe("failed")
    expect((await jobs(schema.conn.query, sql`task = 't'`)).map(r => [r.id, r.state])).toEqual([
      [claim.id, "failed"],
      [pending.id, "pending"],
    ])
    expect(await failureJobs()).toHaveLength(1)
  })

  test("a retry of the application and a later failure of the job insert another failure job with another id", async () => {
    await failAt(await claimed({ maxAttempts: 1 }))
    const source = only(await jobs(schema.conn.query, sql`task = 't'`))
    expect(await retryFailedJob(schema.conn, schema.table, { id: source.id, now: T(3) })).toEqual({
      retried: true,
    })
    const again = only(await claimPlain(schema.conn.query, { ...stamping, limit: 1, now: T(4) }))
    expect(await failAt(again, { now: T(5) })).toBe("failed")
    const ids = (await failureJobs()).map(r => (r.payload as { id: string }).id)
    expect(ids).toHaveLength(2)
    expect(new Set(ids).size).toBe(2)
  })
})

describe("outcomes without a failure job", () => {
  test("a retry inserts nothing", async () => {
    expect(await failAt(await claimed({ maxAttempts: 2 }))).toBe("retry")
    expect(await failureJobs()).toEqual([])
  })

  test("a requeue on the last attempt and a later fail of its stale claim insert nothing", async () => {
    const last = await claimed({ maxAttempts: 1 })
    expect(await requeue(schema.db, last, { now: T(1) })).toBe("requeued")
    expect(await failAt(last)).toBe("fenced")
    expect(await failureJobs()).toEqual([])
  })

  test("a terminal fail of a claim without a stamp inserts nothing", async () => {
    const plain = { ...stamping, failureMaxAttempts: undefined }
    expect(await failAt(await claimed({ maxAttempts: 1 }, plain))).toBe("failed")
    expect(await failureJobs()).toEqual([])
  })

  for (const [mode, outcome] of [
    ["replace", "superseded"],
    ["debounce", "superseded"],
    ["keep", "replaced-pending"],
  ] as const) {
    test(`a merge with a pending job of key mode ${mode} inserts nothing`, async () => {
      const claim = await claimed({ key: "k", maxAttempts: 2 })
      const merge: { mode: KeyMode; maxWait?: number; delay?: number } =
        mode === "debounce" ? { mode, delay: 5, maxWait: 10 } : { mode }
      await enqueue(schema.conn, schema.table, { task: "t", key: "k", now: T(1), ...merge })
      expect(await failAt(claim)).toBe(outcome)
      expect(await failureJobs()).toEqual([])
    })
  }
})

describe("atomicity and uniqueness", () => {
  test("a fail that rolls back after its statements leaves the job active and inserts no failure job", async () => {
    const claim = await claimed({ maxAttempts: 1 })
    const rollback = new Error("rollback")
    const outcome = failAt(claim, {
      pause: {
        beforeCommit: () => {
          throw rollback
        },
      },
    })
    await expect(outcome).rejects.toBe(rollback)
    expect((await jobs(schema.conn.query)).map(r => [r.task, r.state])).toEqual([["t", "active"]])
  })

  test("a rejected insert of the failure job rejects the fail and leaves the job active", async () => {
    await schema.conn.query`
      alter table ${workerSchema.job} add constraint test_reject check (task <> 't:failure')`
    const claim = await claimed({ maxAttempts: 1 })
    const error = await failAt(claim).catch((e: unknown) => e)
    expect(error).toBeInstanceOf(QueryError)
    expect((error as QueryError).constraint).toBe("test_reject")
    expect((await jobs(schema.conn.query)).map(r => [r.task, r.state])).toEqual([["t", "active"]])
  })

  test("a fail after the reaper failed the job is fenced and inserts no second failure job", async () => {
    const claim = await claimed({ maxAttempts: 1 })
    expect(await reap(schema.conn.query, { now: T(11) })).toEqual([
      { id: claim.id, state: "failed", detached: false },
    ])
    expect(await failAt(claim, { now: T(12) })).toBe("fenced")
    expect(await failureJobs()).toHaveLength(1)
  })

  test("the reaper skips a job whose fail holds it, and the fail inserts the only failure job", async () => {
    const claim = await claimed({ maxAttempts: 1 })
    let reaped: unknown
    expect(
      await failAt(claim, {
        now: T(11),
        pause: {
          afterOwnLock: async () => {
            reaped = await reap(schema.conn.query, { now: T(11) })
          },
        },
      }),
    ).toBe("failed")
    expect(reaped).toEqual([])
    expect(await reap(schema.conn.query, { now: T(12) })).toEqual([])
    expect(only(await failureJobs()).payload).toMatchObject({ reason: "handler-error" })
  })
})

describe("reaper", () => {
  test("a stamped job whose lease expired on its last attempt fails and inserts its failure job, and the reap reports only the job", async () => {
    const claim = await claimed({ scope: "s1", payload: 5, maxAttempts: 1 })
    expect(await reap(schema.conn.query, { now: T(11) })).toEqual([
      { id: claim.id, state: "failed", detached: false },
    ])
    const job = only(await failureJobs())
    expect([job.scope, job.maxAttempts, job.runAt]).toEqual(["s1", 7, T(11)])
    expect(job.payload).toEqual({
      id: claim.claimId,
      reason: "lease-expired",
      error: { message: "lease expired" },
      failedAt: "2030-01-01T00:00:11.000Z",
      job: {
        id: claim.id,
        task: "t",
        scope: "s1",
        attempt: 1,
        maxAttempts: 1,
        key: null,
        queue: null,
      },
      payload: 5,
    })
  })

  test("the return of the jobs of a worker inserts the failure job of its job on its last attempt with reason worker-restarted", async () => {
    await claimed({ maxAttempts: 1 })
    await reap(schema.conn.query, { worker: "w", now: T(1) })
    expect(await jobFailure()).toMatchObject({
      reason: "worker-restarted",
      error: { message: "worker restarted" },
    })
  })

  test("a stamped job that returns to pending inserts nothing", async () => {
    await claimed({ maxAttempts: 2 })
    expect(await reap(schema.conn.query, { now: T(11) })).toHaveLength(1)
    expect(await failureJobs()).toEqual([])
  })

  test("a batch redone row by row after a conflict of a key inserts the failure job once", async () => {
    const failing = await claimed({ maxAttempts: 1 })
    const returning = await claimed({ key: "k", maxAttempts: 5 })
    const reaper = await schema.connect({ hooks: ["reap"] })
    const release = await holdHook(schema.conn, "reap")
    const stats = { uniqueViolations: 0 }
    const reaped = reap(reaper.query, { now: T(11) }, stats)
    await waitForLock(schema.conn, reaper.pid)
    await enqueue(schema.conn, schema.table, { task: "t", key: "k", now: T(11) })
    await release()
    expect(await reaped).toEqual([
      { id: failing.id, state: "failed", detached: false },
      { id: returning.id, state: "pending", detached: true },
    ])
    expect(stats.uniqueViolations).toBe(1)
    expect(only(await failureJobs()).payload).toMatchObject({ id: failing.claimId })
  })

  test("a rejected insert of a failure job rejects the reap, which leaves the rows of its batch active", async () => {
    await schema.conn.query`
      alter table ${workerSchema.job} add constraint test_reject check (task <> 't:failure')`
    const rejected = await claimed({ maxAttempts: 1 })
    // A job that returns to pending inserts nothing, and stays active with the batch too.
    const other = await claimed({ maxAttempts: 5 })
    await expect(reap(schema.conn.query, { now: T(11) })).rejects.toMatchObject({
      constraint: "test_reject",
    })
    expect((await jobs(schema.conn.query)).map(r => [r.id, r.state])).toEqual([
      [rejected.id, "active"],
      [other.id, "active"],
    ])
  })
})
