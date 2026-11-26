import { describe, expectTypeOf, test } from "bun:test"

import type { AnyDb } from "kern-orm"
import type { Database } from "kern-orm/db"
import type pg from "pg"
import type postgres from "postgres"

import type { EnqueueResult } from "../sql/enqueue"
import { type AnyTask, defineTask } from "../task/task"

import type { RetryResult } from "./administration"
import { ALL_SCOPES, createJobs } from "./jobs"

declare const jobs: ReturnType<typeof createJobs>

const sendEmail = defineTask("email.send").withPayload<{ to: string }>()
const purgeExpiredSessions = defineTask("sessions.purgeExpired")
const notify = defineTask("notify").withPayload<{ id: string } | undefined>()
const publishPosts = defineTask("posts.publish", { key: true }).withBatchPayload<number>({
  maxItems: 100,
})

describe("createJobs", () => {
  test("takes a client, a client of a transaction, a function that returns one, a Database of kern-db, or a Db of kern-orm", () => {
    const pool = {} as pg.Pool
    const sql = {} as postgres.Sql
    const tx = {} as postgres.TransactionSql
    createJobs(pool)
    createJobs(sql)
    createJobs({} as pg.PoolClient)
    createJobs(tx)
    createJobs(() => tx)
    createJobs({} as Database, { schemaName: "jobs" })
    createJobs({} as AnyDb)
    // @ts-expect-error A connection string is not a client.
    createJobs("postgres://localhost")
  })

  test("takes a scope, a function that returns one, ALL_SCOPES, null, or undefined", () => {
    const pool = {} as pg.Pool
    createJobs(pool, { scope: "tenant" })
    createJobs(pool, { scope: () => "tenant" })
    createJobs(pool, { scope: ALL_SCOPES })
    // @ts-expect-error The function returns a string.
    createJobs(pool, { scope: () => null })
    // @ts-expect-error The function returns a string.
    createJobs(pool, { scope: () => ALL_SCOPES })
    // @ts-expect-error Only ALL_SCOPES is a symbol of a scope.
    createJobs(pool, { scope: Symbol("kern-jobs.ALL_SCOPES") })
    createJobs(pool, { scope: null })
    createJobs(pool, { scope: undefined })
    // @ts-expect-error A scope is a string.
    createJobs(pool, { scope: 42 })
  })
})

describe("enqueue", () => {
  test("takes the payload of the task", () => {
    expectTypeOf(jobs.enqueue(sendEmail, { to: "a@example.com" })).toEqualTypeOf<
      Promise<EnqueueResult>
    >()
    // @ts-expect-error The task needs a payload.
    void jobs.enqueue(sendEmail)
    // @ts-expect-error The payload lacks `to`.
    void jobs.enqueue(sendEmail, {})
    // @ts-expect-error The payload has a field of another type.
    void jobs.enqueue(sendEmail, { to: 1 })
  })

  test("makes the payload optional when it can be undefined", () => {
    void jobs.enqueue(purgeExpiredSessions)
    void jobs.enqueue(purgeExpiredSessions, undefined, { delay: 5 })
    void jobs.enqueue(notify)
    // @ts-expect-error The task has no payload.
    void jobs.enqueue(purgeExpiredSessions, { id: "1" })
  })

  test("takes overrides of the options, without the settings of the task", () => {
    void jobs.enqueue(sendEmail, { to: "a@example.com" }, { delay: 60, key: null })
    const overrides = { key: "k", lease: 5 }
    // @ts-expect-error The lease belongs to defineTask.
    void jobs.enqueue(sendEmail, { to: "a@example.com" }, overrides)
    // @ts-expect-error delay and runAt exclude each other.
    void jobs.enqueue(sendEmail, { to: "a@example.com" }, { delay: 60, runAt: new Date() })
  })

  test("rejects a batch task", () => {
    // @ts-expect-error enqueueItems enqueues the items of a batch task.
    void jobs.enqueue(publishPosts, [1])
  })
})

describe("enqueueMany", () => {
  test("takes an array of payloads of the task and overrides, and resolves to a result of each", () => {
    expectTypeOf(jobs.enqueueMany(sendEmail, [{ to: "a@example.com" }])).toEqualTypeOf<
      Promise<EnqueueResult[]>
    >()
    // @ts-expect-error enqueueItems enqueues the items of a batch task.
    void jobs.enqueueMany(publishPosts, [[1], [2, 3]])
    const payloads: readonly { to: string }[] = [{ to: "a@example.com" }]
    void jobs.enqueueMany(sendEmail, payloads, { delay: 60 })
    void jobs.enqueueMany(purgeExpiredSessions, [undefined])
    // @ts-expect-error A payload has a field of another type.
    void jobs.enqueueMany(sendEmail, [{ to: "a@example.com" }, { to: 1 }])
    // @ts-expect-error The payloads come in an array.
    void jobs.enqueueMany(sendEmail, { to: "a@example.com" })
    // @ts-expect-error The lease belongs to defineTask.
    void jobs.enqueueMany(sendEmail, payloads, { lease: 5 })
  })
})

describe("enqueueItems", () => {
  test("takes an array of the items of a batch task and a key as its override, and resolves to a result of each job", () => {
    expectTypeOf(jobs.enqueueItems(publishPosts, [1])).toEqualTypeOf<Promise<EnqueueResult[]>>()
    const items: readonly number[] = [1, 2]
    void jobs.enqueueItems(publishPosts, items, { key: "site:1" })
    void jobs.enqueueItems(publishPosts, [])
    // @ts-expect-error The items are an array.
    void jobs.enqueueItems(publishPosts, 1)
    // @ts-expect-error An item has another type.
    void jobs.enqueueItems(publishPosts, ["1"])
    // @ts-expect-error The items append to the pending job of their key.
    void jobs.enqueueItems(publishPosts, [1], { key: null })
    // @ts-expect-error The other options apply to the whole batch and come from defineTask.
    void jobs.enqueueItems(publishPosts, [1], { delay: 5 })
    const overrides = { key: "site:1", delay: 5 }
    // @ts-expect-error The other options are rejected in an object of the overrides of other enqueues too.
    void jobs.enqueueItems(publishPosts, [1], overrides)
    // @ts-expect-error enqueue enqueues a task that is not a batch task.
    void jobs.enqueueItems(sendEmail, [{ to: "a@example.com" }])
  })

  test("takes the task of an implementation, of its own kind, and a task of either kind that maxItems tells apart", () => {
    void jobs.enqueue(sendEmail.implement(() => {}).task, { to: "a@example.com" })
    void jobs.enqueueItems(publishPosts.implement(() => {}).task, [1])
    const either = publishPosts as AnyTask<"posts.publish", number[]>
    // @ts-expect-error The task may be a batch task.
    void jobs.enqueue(either, [1])
    if (either.maxItems === undefined) void jobs.enqueue(either, [1])
    else void jobs.enqueueItems(either, [1])
    const plain = sendEmail as AnyTask<"email.send", { to: string }>
    void jobs.enqueue(plain, { to: "a@example.com" })
    const pair = defineTask("t").withPayload<[number, string]>() as AnyTask<"t", [number, string]>
    void jobs.enqueue(pair, [1, "a"])
  })
})

describe("retry and delete", () => {
  test("take the id of a job as a string, as an enqueue returns it", () => {
    expectTypeOf(jobs.retry("1")).toEqualTypeOf<Promise<RetryResult | null>>()
    expectTypeOf(jobs.delete("1")).toEqualTypeOf<Promise<boolean>>()
    // @ts-expect-error An id is a string.
    void jobs.retry(1)
  })

  test("a retry tells the pending job of the key only when the failed job did not return", async () => {
    const result = await jobs.retry("1")
    if (result && !result.retried) expectTypeOf(result.pendingId).toEqualTypeOf<string>()
    // @ts-expect-error A retried job has no pending job of its key.
    if (result?.retried) void result.pendingId
  })
})

describe("deletePending", () => {
  test("takes no filter, a task of any payload, scoped or not, or a task and a key, and resolves to a count", () => {
    expectTypeOf(jobs.deletePending()).toEqualTypeOf<Promise<number>>()
    // @ts-expect-error A filter names a task.
    void jobs.deletePending({})
    // @ts-expect-error Only a call without an argument deletes the jobs of every task.
    void jobs.deletePending(undefined)
    void jobs.deletePending({ task: sendEmail, key: "k" })
    void jobs.deletePending({ task: sendEmail, key: true })
    void jobs.deletePending({ task: purgeExpiredSessions })
    void jobs.deletePending({ task: defineTask("search.indexProduct", { scoped: true }), key: "k" })
    void jobs.deletePending({ task: publishPosts, key: "k" })
    // @ts-expect-error A key is a string or true.
    void jobs.deletePending({ task: sendEmail, key: null })
    // @ts-expect-error A key needs its task.
    void jobs.deletePending({ key: "k" })
  })
})

describe("deleteFailed", () => {
  test("takes no filter, or a task of any payload, scoped or not, olderThan, or both, and resolves to a count", () => {
    expectTypeOf(jobs.deleteFailed()).toEqualTypeOf<Promise<number>>()
    // @ts-expect-error A filter names a task, olderThan, or both.
    void jobs.deleteFailed({})
    // @ts-expect-error Only a call without an argument deletes the jobs of every task at any age.
    void jobs.deleteFailed(undefined)
    void jobs.deleteFailed({ task: sendEmail })
    void jobs.deleteFailed({ olderThan: 60 })
    void jobs.deleteFailed({
      task: defineTask("search.indexProduct", { scoped: true }),
      olderThan: 60,
    })
    // @ts-expect-error olderThan is a number of seconds.
    void jobs.deleteFailed({ olderThan: "30 days" })
    // @ts-expect-error A key belongs to deletePending.
    void jobs.deleteFailed({ task: sendEmail, key: "k" })
  })
})
