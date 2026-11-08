// Administration of jobs: a retry of a failed job with and without a pending job of its key, in a scope, and its races with producers, claims, other retries, fail, and the reaper; and a delete of a pending or failed job in a scope, and its races with claims, producers, and fail.
import { afterEach, beforeEach, describe, expect, test } from "bun:test"

import { waitForLock } from "kern-db/testing"

import { deleteJob, retryFailedJob } from "./administration"
import { claimCycle, claimPlain } from "./claim"
import { enqueue, type EnqueueParams } from "./enqueue"
import type { KeyMode } from "./job"
import { reap } from "./lease"
import { fail } from "./settlement"
import { query } from "./statement"
import { createTestSchema, holdHook, jobs, only, T, type TestSchema } from "./testing"
import { isUniqueViolation } from "./violation"

let schema: TestSchema
beforeEach(async () => {
  schema = await createTestSchema()
})
afterEach(async () => {
  await schema.dispose()
})

const W = { worker: "w1", tasks: ["t"] }

/** Options of an enqueue in each key mode; `debounce` takes `delay` and `maxWait`. */
const modeOptions = (mode: KeyMode): Partial<EnqueueParams> =>
  mode === "debounce" ? { mode, delay: 5, maxWait: 50 } : { mode }

/** Enqueues a job of task `t` at T(0), claims it at T(10), fails it for good at T(11), and returns its id. */
async function createFailedJob(o: Partial<EnqueueParams> = {}): Promise<string> {
  const { id } = await enqueue(schema.admin, schema.table, {
    task: "t",
    payload: "failed",
    now: T(0),
    ...o,
  })
  const claim = only(await claimCycle(schema.admin, schema.table, { ...W, slots: 1, now: T(10) }))
  expect(
    await fail(schema.database, schema.table, claim, {
      error: "boom",
      terminal: true,
      backoff: 0,
      now: T(11),
    }),
  ).toBe("failed")
  return id
}

const retry = (id: string, o: { scope?: string; now?: Date } = {}) =>
  retryFailedJob(schema.admin, schema.table, { id, now: T(20), ...o })

/** Returns the error with which a promise rejects, and throws when it resolves. */
async function getRejection(promise: Promise<unknown>): Promise<unknown> {
  try {
    await promise
  } catch (error) {
    return error
  }
  throw new Error("the promise resolved")
}

describe("retry", () => {
  test("returns a failed job to pending in place, with a full budget of attempts and no backoff, and keeps its other columns", async () => {
    await enqueue(schema.admin, schema.table, {
      task: "t",
      key: "k",
      mode: "keep",
      queue: "q",
      payload: { n: 1 },
      maxAttempts: 2,
      now: T(0),
    })
    const first = only(await claimCycle(schema.admin, schema.table, { ...W, slots: 1, now: T(1) }))
    await fail(schema.database, schema.table, first, { error: "first", backoff: 3, now: T(2) })
    const last = only(await claimCycle(schema.admin, schema.table, { ...W, slots: 1, now: T(5) }))
    await fail(schema.database, schema.table, last, { error: "boom", backoff: 3, now: T(6) })
    const failed = only(await jobs(schema))
    expect(failed).toMatchObject({
      state: "failed",
      attempts: 2,
      key: "k",
      key_mode: "keep",
      queue: "q",
      payload: { n: 1 },
      max_attempts: 2,
      created_at: T(0),
      locked_at: T(5),
      last_error: "boom",
      failed_at: T(6),
    })

    expect(await retry(failed.id, { now: T(10) })).toEqual({ retried: true })
    expect(only(await jobs(schema))).toEqual({
      ...failed,
      state: "pending",
      attempts: 0,
      run_at: T(10),
      not_before: null,
      window_started_at: T(10),
      failed_at: null,
    })
  })

  test("a later enqueue with the key merges into the retried job", async () => {
    const id = await createFailedJob({ key: "k" })
    await retry(id)
    expect(
      await enqueue(schema.admin, schema.table, {
        task: "t",
        key: "k",
        payload: "event",
        now: T(21),
      }),
    ).toEqual({ id, created: false })
    expect((await jobs(schema)).map(r => [r.id, r.state, r.payload])).toEqual([
      [id, "pending", "event"],
    ])
  })

  for (const mode of ["replace", "keep", "debounce"] as KeyMode[]) {
    test(`a pending job of the key in mode ${mode} keeps the failed job from returning, and both stay as they are`, async () => {
      // The failed job has another key mode and budget, so that a retry that changed the pending job by it would show.
      const failed = await createFailedJob({
        key: "k",
        maxAttempts: 1,
        ...modeOptions(mode === "keep" ? "replace" : "keep"),
      })
      await enqueue(schema.admin, schema.table, {
        task: "t",
        key: "k",
        payload: "pending",
        maxAttempts: 5,
        now: T(12),
        ...modeOptions(mode),
      })
      // The pending job has spent an attempt and waits for its backoff.
      const claim = only(
        await claimCycle(schema.admin, schema.table, { ...W, slots: 1, now: T(30) }),
      )
      await fail(schema.database, schema.table, claim, {
        error: "retrying",
        backoff: 40,
        now: T(31),
      })
      const before = await jobs(schema)
      expect(before.map(r => [r.id, r.state, r.key_mode, r.attempts, r.not_before])).toEqual([
        [failed, "failed", mode === "keep" ? "replace" : "keep", 1, null],
        [claim.id, "pending", mode, 1, T(71)],
      ])
      expect(await retry(failed, { now: T(40) })).toEqual({ retried: false, pendingId: claim.id })
      expect(await jobs(schema)).toEqual(before)
    })
  }

  test("a conflict with a pending job of the key leaves the transaction of the client usable", async () => {
    const failed = await createFailedJob({ key: "k" })
    const { id: pending } = await enqueue(schema.admin, schema.table, {
      task: "t",
      key: "k",
      now: T(12),
    })
    const operator = await schema.connect()
    await query(operator, "begin")
    expect(await retryFailedJob(operator, schema.table, { id: failed })).toEqual({
      retried: false,
      pendingId: pending,
    })
    expect(await deleteJob(operator, schema.table, { id: failed })).toBe(true)
    await query(operator, "commit")
    expect((await jobs(schema)).map(r => r.id)).toEqual([pending])
  })

  test("a pending job of the key in another scope does not keep the failed job from returning", async () => {
    const failed = await createFailedJob({ key: "k", scope: "a" })
    await enqueue(schema.admin, schema.table, { task: "t", key: "k", scope: "b", now: T(12) })
    expect(await retry(failed)).toEqual({ retried: true })
    expect((await jobs(schema)).map(r => [r.scope, r.key, r.state])).toEqual([
      ["a", "k", "pending"],
      ["b", "k", "pending"],
    ])
  })

  test("acts only on a failed job of the scope that it is given", async () => {
    const failed = await createFailedJob({ scope: "a" })
    expect(await retry(failed, { scope: "b" })).toBeNull()
    expect(only(await jobs(schema)).state).toBe("failed")
    expect(await retry(failed, { scope: "a" })).toEqual({ retried: true })
  })

  test("resolves to null and changes nothing for a pending, active, or unknown id", async () => {
    const { id: active } = await enqueue(schema.admin, schema.table, { task: "t", now: T(0) })
    await claimPlain(schema.admin, schema.table, { ...W, limit: 1, now: T(1) })
    const { id: pending } = await enqueue(schema.admin, schema.table, { task: "t", now: T(2) })
    const before = await jobs(schema)
    for (const id of [active, pending, "999999"]) expect(await retry(id)).toBeNull()
    expect(await jobs(schema)).toEqual(before)
  })

  for (const end of ["commit", "rollback"] as const) {
    test(`waits for an uncommitted insert of a pending job of the key; producer ${end}`, async () => {
      const failed = await createFailedJob({ key: "k" })
      const producer = await schema.connect()
      const operator = await schema.connect()
      await query(producer, "begin")
      const { id: pending } = await enqueue(producer, schema.table, {
        task: "t",
        key: "k",
        now: T(12),
      })
      const retried = retryFailedJob(operator, schema.table, { id: failed, now: T(20) })
      await waitForLock(schema.admin, operator.pid)
      await query(producer, end)
      if (end === "commit") {
        // The snapshot of the retry missed the pending job, so the unique index rejects the update, which changes nothing.
        expect(isUniqueViolation(await getRejection(retried), "job_pending_key_idx")).toBe(true)
        expect((await jobs(schema)).map(r => [r.id, r.state])).toEqual([
          [failed, "failed"],
          [pending, "pending"],
        ])
      } else {
        expect(await retried).toEqual({ retried: true })
        expect((await jobs(schema)).map(r => [r.id, r.state, r.key])).toEqual([
          [failed, "pending", "k"],
        ])
      }
    })
  }

  test("a pending job of the key that a producer commits after the snapshot of the retry makes it reject with a unique violation and change nothing", async () => {
    const failed = await createFailedJob({ key: "k" })
    const operator = await schema.connect({ hooks: ["retry"] })
    const release = await holdHook(schema.admin, "retry")
    const retried = retryFailedJob(operator, schema.table, { id: failed, now: T(20) })
    await waitForLock(schema.admin, operator.pid)
    const { id: pending } = await enqueue(schema.admin, schema.table, {
      task: "t",
      key: "k",
      now: T(12),
    })
    await release()
    expect(isUniqueViolation(await getRejection(retried), "job_pending_key_idx")).toBe(true)
    expect((await jobs(schema)).map(r => [r.id, r.state])).toEqual([
      [failed, "failed"],
      [pending, "pending"],
    ])
  })

  test("waits for an uncommitted claim of the pending job of the key, and then returns the failed job to pending with its key", async () => {
    const failed = await createFailedJob({ key: "k" })
    const { id: claimed } = await enqueue(schema.admin, schema.table, {
      task: "t",
      key: "k",
      now: T(12),
    })
    const worker = await schema.connect()
    const operator = await schema.connect()
    await query(worker, "begin")
    only(await claimPlain(worker, schema.table, { ...W, limit: 1, now: T(13) }))
    const retried = retryFailedJob(operator, schema.table, { id: failed, now: T(20) })
    await waitForLock(schema.admin, operator.pid)
    await query(worker, "commit")
    expect(await retried).toEqual({ retried: true })
    expect((await jobs(schema)).map(r => [r.id, r.state, r.key])).toEqual([
      [failed, "pending", "k"],
      [claimed, "active", "k"],
    ])
  })

  test("of two concurrent retries of a job, the second waits for the first and finds no failed job", async () => {
    const failed = await createFailedJob()
    const operator1 = await schema.connect()
    const operator2 = await schema.connect()
    await query(operator1, "begin")
    expect(await retryFailedJob(operator1, schema.table, { id: failed })).toEqual({ retried: true })
    const retried = retryFailedJob(operator2, schema.table, { id: failed })
    await waitForLock(schema.admin, operator2.pid)
    await query(operator1, "commit")
    expect(await retried).toBeNull()
    expect((await jobs(schema)).map(r => [r.id, r.state])).toEqual([[failed, "pending"]])
  })

  test("a retry during the fail transaction of an active job of the key, after its competitor check: fail retries after a unique violation and yields to the retried job", async () => {
    const failed = await createFailedJob({ key: "k" })
    await enqueue(schema.admin, schema.table, { task: "t", key: "k", now: T(12) })
    const claim = only(await claimPlain(schema.admin, schema.table, { ...W, limit: 1, now: T(13) }))
    const stats = { uniqueViolations: 0 }
    let once = false
    const outcome = await fail(
      schema.database,
      schema.table,
      claim,
      {
        error: "boom",
        backoff: 30,
        now: T(14),
        pause: {
          afterCompetitorCheck: async () => {
            if (once) return
            once = true
            expect(await retry(failed)).toEqual({ retried: true })
          },
        },
      },
      stats,
    )
    expect([outcome, stats.uniqueViolations]).toEqual(["superseded", 1])
    expect((await jobs(schema)).map(r => [r.id, r.state])).toEqual([[failed, "pending"]])
  })

  test("a retry after the snapshot of the reaper: the reaper retries after a unique violation and returns its job without the key", async () => {
    const failed = await createFailedJob({ key: "k" })
    await enqueue(schema.admin, schema.table, { task: "t", key: "k", now: T(12) })
    const expired = only(
      await claimPlain(schema.admin, schema.table, { ...W, defaultLease: 1, limit: 1, now: T(13) }),
    )
    const reaper = await schema.connect({ hooks: ["reap"] })
    const release = await holdHook(schema.admin, "reap")
    const stats = { uniqueViolations: 0 }
    const reaped = reap(reaper, schema.table, { now: T(20) }, stats)
    await waitForLock(schema.admin, reaper.pid)
    expect(await retry(failed)).toEqual({ retried: true })
    await release()
    expect(await reaped).toEqual([{ id: expired.id, state: "pending", detached: true }])
    expect(stats.uniqueViolations).toBe(1)
    expect((await jobs(schema)).map(r => [r.id, r.key])).toEqual([
      [failed, "k"],
      [expired.id, null],
    ])
  })
})

describe("delete", () => {
  const deleteIn = (id: string, scope?: string) =>
    deleteJob(schema.admin, schema.table, { id, scope })

  test("deletes a pending or failed job, and resolves to false for an active or unknown id, which stays as it is", async () => {
    const failed = await createFailedJob()
    const { id: active } = await enqueue(schema.admin, schema.table, { task: "t", now: T(12) })
    await claimPlain(schema.admin, schema.table, { ...W, limit: 1, now: T(13) })
    const { id: pending } = await enqueue(schema.admin, schema.table, { task: "t", now: T(14) })
    const before = await jobs(schema, `state = 'active'`)
    expect([await deleteIn(failed), await deleteIn(pending)]).toEqual([true, true])
    for (const id of [active, failed, pending, "999999"]) expect(await deleteIn(id)).toBe(false)
    expect(await jobs(schema)).toEqual(before)
  })

  test("acts only on a job of the scope that it is given", async () => {
    const { id } = await enqueue(schema.admin, schema.table, { task: "t", scope: "a", now: T(0) })
    expect(await deleteIn(id, "b")).toBe(false)
    expect(await deleteIn(id, "a")).toBe(true)
  })

  test("waits for an uncommitted claim of a pending job, and then resolves to false", async () => {
    const { id } = await enqueue(schema.admin, schema.table, { task: "t", now: T(0) })
    const worker = await schema.connect()
    const operator = await schema.connect()
    await query(worker, "begin")
    only(await claimPlain(worker, schema.table, { ...W, limit: 1, now: T(1) }))
    const deleted = deleteJob(operator, schema.table, { id })
    await waitForLock(schema.admin, operator.pid)
    await query(worker, "commit")
    expect(await deleted).toBe(false)
    expect((await jobs(schema)).map(r => [r.id, r.state])).toEqual([[id, "active"]])
  })

  test("waits for a producer that merges an event into a pending job, and then deletes the job with the event", async () => {
    const { id } = await enqueue(schema.admin, schema.table, {
      task: "t",
      key: "k",
      payload: 1,
      now: T(0),
    })
    const producer = await schema.connect()
    const operator = await schema.connect()
    await query(producer, "begin")
    expect(
      await enqueue(producer, schema.table, { task: "t", key: "k", payload: 2, now: T(1) }),
    ).toEqual({ id, created: false })
    const deleted = deleteJob(operator, schema.table, { id })
    await waitForLock(schema.admin, operator.pid)
    await query(producer, "commit")
    expect(await deleted).toBe(true)
    expect(await jobs(schema)).toEqual([])
  })

  for (const [mode, outcome, deleted] of [
    ["keep", "replaced-pending", false],
    ["replace", "superseded", true],
  ] as const) {
    test(`waits for a fail transaction that holds a pending job as the pending job of its key in mode ${mode}`, async () => {
      await enqueue(schema.admin, schema.table, { task: "t", key: "k", mode, now: T(0) })
      const claim = only(
        await claimPlain(schema.admin, schema.table, { ...W, limit: 1, now: T(1) }),
      )
      const { id: pending } = await enqueue(schema.admin, schema.table, {
        task: "t",
        key: "k",
        mode,
        now: T(2),
      })
      const operator = await schema.connect()
      let deletion: Promise<boolean> | undefined
      const failed = fail(schema.database, schema.table, claim, {
        error: "boom",
        backoff: 30,
        now: T(3),
        pause: {
          beforeCommit: async () => {
            deletion = deleteJob(operator, schema.table, { id: pending })
            await waitForLock(schema.admin, operator.pid)
          },
        },
      })
      expect(await failed).toBe(outcome)
      expect(await deletion).toBe(deleted)
      // With keep, fail deleted the pending job and returned its own; with replace, it deleted its own, and the delete took the pending one.
      expect((await jobs(schema)).map(r => [r.id, r.state])).toEqual(
        mode === "keep" ? [[claim.id, "pending"]] : [],
      )
    })
  }
})
