// Administration of jobs: a retry of a failed job, and a delete of pending or failed jobs by id or by a filter, in a scope, and their races with producers, claims, retries, fail, and the reaper.
import { afterEach, beforeEach, describe, expect, test } from "bun:test"

import { waitForLock } from "kern-db/testing"

import { deleteFailedJobs, deleteJob, deletePendingJobs, retryFailedJob } from "./administration"
import { claimCycle, claimPlain } from "./claim"
import { enqueue, type EnqueueParams } from "./enqueue"
import type { KeyMode } from "./job"
import { reap } from "./lease"
import { fail } from "./settlement"
import { query } from "./statement"
import { type Conn, createTestSchema, holdHook, jobs, only, T, type TestSchema } from "./testing"
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

/**
 * Enqueues a job, of task `t` unless `o` gives another, 11 seconds before `failedAt`, claims it a second before `failedAt`, fails it for good at `failedAt`, and returns its id.
 * With the default `failedAt`, the job is enqueued at T(0), claimed at T(10), and fails at T(11).
 */
async function createFailedJob(o: Partial<EnqueueParams> = {}, failedAt = 11): Promise<string> {
  const { id } = await enqueue(schema.admin, schema.table, {
    task: "t",
    payload: "failed",
    now: T(failedAt - 11),
    ...o,
  })
  const tasks = [o.task ?? "t"]
  const claim = only(
    await claimCycle(schema.admin, schema.table, { ...W, tasks, slots: 1, now: T(failedAt - 1) }),
  )
  expect(
    await fail(schema.database, schema.table, claim, {
      error: "boom",
      reason: "terminal-error",
      backoff: 0,
      now: T(failedAt),
    }),
  ).toBe("failed")
  return id
}

const retry = (id: string, o: { scope?: string; now?: Date } = {}) =>
  retryFailedJob(schema.admin, schema.table, { id, now: T(20), ...o })

/**
 * Outcomes of a fail with attempts left of a job of a key while a delete waits for the pending job of the key that the fail transaction holds.
 * With keep, fail deletes the pending job and returns its own to pending, so the delete finds no row; with replace, fail deletes its own job, and the delete takes the pending one.
 */
const failRaces = [
  { mode: "keep", outcome: "replaced-pending", deleted: false },
  { mode: "replace", outcome: "superseded", deleted: true },
] as const

/** Ids and states of the jobs that a fail race leaves. */
const getFailRaceRows = (mode: KeyMode, claimedJobId: string) =>
  mode === "keep" ? [[claimedJobId, "pending"]] : []

/**
 * Fails a claimed job of a key with attempts left, and starts `remove` of the pending job of the key on a separate connection and waits until it blocks on the pending row before letting the fail transaction commit.
 * Returns the id of the claimed job, the outcome of the fail, the result of `remove`, and the ids and states of the jobs left.
 */
async function deleteDuringFail<R>(
  mode: KeyMode,
  remove: (operator: Conn, pendingId: string) => Promise<R>,
) {
  await enqueue(schema.admin, schema.table, { task: "t", key: "k", mode, now: T(0) })
  const claim = only(await claimPlain(schema.admin, schema.table, { ...W, limit: 1, now: T(1) }))
  const { id: pending } = await enqueue(schema.admin, schema.table, {
    task: "t",
    key: "k",
    mode,
    now: T(2),
  })
  const operator = await schema.connect()
  let deletion: Promise<R> | undefined
  const outcome = await fail(schema.database, schema.table, claim, {
    error: "boom",
    backoff: 30,
    now: T(3),
    pause: {
      beforeCommit: async () => {
        deletion = remove(operator, pending)
        await waitForLock(schema.admin, operator.pid)
      },
    },
  })
  return {
    claimedJobId: claim.id,
    outcome,
    deleted: await deletion,
    left: (await jobs(schema)).map(r => [r.id, r.state]),
  }
}

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

  for (const { mode, outcome, deleted } of failRaces) {
    test(`waits for a fail transaction that holds a pending job as the pending job of its key in mode ${mode}, and then resolves to ${deleted}`, async () => {
      const { claimedJobId, ...race } = await deleteDuringFail(mode, (operator, id) =>
        deleteJob(operator, schema.table, { id }),
      )
      expect(race).toEqual({ outcome, deleted, left: getFailRaceRows(mode, claimedJobId) })
    })
  }
})

describe("delete pending", () => {
  const deletePending = (filter: Parameters<typeof deletePendingJobs>[2]) =>
    deletePendingJobs(schema.admin, schema.table, filter)

  test("deletes only the pending job of the key in its scope and task, and returns 0 when there is none", async () => {
    const failed = await createFailedJob({ key: "k", scope: "a" })
    await enqueue(schema.admin, schema.table, { task: "t", key: "k", scope: "a", now: T(12) })
    only(await claimPlain(schema.admin, schema.table, { ...W, limit: 1, now: T(13) }))
    const { id: pending } = await enqueue(schema.admin, schema.table, {
      task: "t",
      key: "k",
      scope: "a",
      now: T(14),
    })
    for (const o of [{ scope: "b" }, { task: "u" }, { key: "other" }]) {
      await enqueue(schema.admin, schema.table, {
        task: "t",
        key: "k",
        scope: "a",
        ...o,
        now: T(14),
      })
    }
    const others = (await jobs(schema)).filter(r => r.id !== pending)
    expect(others.map(r => r.state)).toEqual(["failed", "active", "pending", "pending", "pending"])
    expect(others[0]!.id).toBe(failed)
    const filter = { scope: "a", task: "t", key: "k" }
    expect(await deletePending(filter)).toBe(1)
    expect(await jobs(schema)).toEqual(others)
    expect(await deletePending(filter)).toBe(0)
  })

  test("deletes the pending jobs of a scope, of a task, or of every scope, with and without a key, and leaves active and failed jobs", async () => {
    await createFailedJob({ scope: "a" })
    await enqueue(schema.admin, schema.table, { task: "t", scope: "a", now: T(12) })
    only(await claimPlain(schema.admin, schema.table, { ...W, limit: 1, now: T(13) }))
    const enqueueIn = (scope: string, task: string, key?: string) =>
      enqueue(schema.admin, schema.table, { task, scope, key, now: T(14) })
    await enqueueIn("a", "t", "k")
    await enqueueIn("a", "t")
    await enqueueIn("a", "u")
    await enqueueIn("b", "t")
    await enqueueIn("", "t")
    const pending = async () =>
      (await jobs(schema, `state = 'pending'`)).map(r => [r.scope, r.task])
    expect(await deletePending({ scope: "a", task: "t" })).toBe(2)
    expect(await pending()).toEqual([
      ["a", "u"],
      ["b", "t"],
      ["", "t"],
    ])
    expect(await deletePending({ scope: "a" })).toBe(1)
    expect(await deletePending({ task: "t" })).toBe(2)
    expect(await pending()).toEqual([])
    expect((await jobs(schema)).map(r => r.state)).toEqual(["failed", "active"])
    expect(await deletePending({})).toBe(0)
  })

  test("waits for an uncommitted claim of the pending job, and then returns 0", async () => {
    const { id } = await enqueue(schema.admin, schema.table, {
      task: "t",
      key: "k",
      scope: "a",
      now: T(0),
    })
    const worker = await schema.connect()
    const operator = await schema.connect()
    await query(worker, "begin")
    only(await claimPlain(worker, schema.table, { ...W, limit: 1, now: T(1) }))
    const deleted = deletePendingJobs(operator, schema.table, { scope: "a", task: "t", key: "k" })
    await waitForLock(schema.admin, operator.pid)
    await query(worker, "commit")
    expect(await deleted).toBe(0)
    expect((await jobs(schema)).map(r => [r.id, r.state])).toEqual([[id, "active"]])
  })

  for (const { mode, outcome, deleted } of failRaces) {
    const count = Number(deleted)
    test(`waits for a fail transaction that holds the pending job of the key in mode ${mode}, and then returns ${count}`, async () => {
      const { claimedJobId, ...race } = await deleteDuringFail(mode, operator =>
        deletePendingJobs(operator, schema.table, { scope: "", task: "t", key: "k" }),
      )
      expect(race).toEqual({ outcome, deleted: count, left: getFailRaceRows(mode, claimedJobId) })
    })
  }
})

describe("delete failed", () => {
  const deleteFailed = (filter: Parameters<typeof deleteFailedJobs>[2]) =>
    deleteFailedJobs(schema.admin, schema.table, filter)

  test("deletes the failed jobs of a scope, of a task, older than a threshold of age, or of every scope, and leaves pending and active jobs", async () => {
    const oldOfTask = await createFailedJob({ scope: "a" })
    const recentOfTask = await createFailedJob({ scope: "a" }, 100)
    await createFailedJob({ scope: "a", task: "u" })
    await createFailedJob({ scope: "b" })
    await createFailedJob({ scope: "" })
    await enqueue(schema.admin, schema.table, { task: "t", scope: "a", now: T(101) })
    only(await claimPlain(schema.admin, schema.table, { ...W, limit: 1, now: T(102) }))
    await enqueue(schema.admin, schema.table, { task: "t", scope: "a", now: T(103) })
    const failedIds = async () => (await jobs(schema, `state = 'failed'`)).map(r => r.id)
    const before = await failedIds()
    // The bound of a delay: T(110) less 1e12 seconds would be before the earliest timestamp of PostgreSQL.
    expect(await deleteFailed({ olderThan: 1e12, now: T(110) })).toBe(0)
    // The job of the task that failed at T(11) is 89 seconds old at T(100), not older.
    const byAge = { scope: "a", task: "t", now: T(100) }
    expect(await deleteFailed({ ...byAge, olderThan: 89 })).toBe(0)
    expect(await failedIds()).toEqual(before)
    expect(await deleteFailed({ ...byAge, olderThan: 88 })).toBe(1)
    expect(await failedIds()).toEqual(before.filter(id => id !== oldOfTask))
    expect(await deleteFailed({ scope: "a", task: "t" })).toBe(1)
    expect(await failedIds()).not.toContain(recentOfTask)
    expect(await deleteFailed({ scope: "a" })).toBe(1)
    expect(await deleteFailed({ scope: "" })).toBe(1)
    expect(await deleteFailed({})).toBe(1)
    expect((await jobs(schema)).map(r => r.state)).toEqual(["active", "pending"])
  })

  test("waits for an uncommitted retry of the failed job, and then returns 0", async () => {
    const failed = await createFailedJob()
    const operator1 = await schema.connect()
    const operator2 = await schema.connect()
    await query(operator1, "begin")
    expect(await retryFailedJob(operator1, schema.table, { id: failed })).toEqual({ retried: true })
    const deleted = deleteFailedJobs(operator2, schema.table, { task: "t" })
    await waitForLock(schema.admin, operator2.pid)
    await query(operator1, "commit")
    expect(await deleted).toBe(0)
    expect((await jobs(schema)).map(r => [r.id, r.state])).toEqual([[failed, "pending"]])
  })
})
