import { describe, expectTypeOf, test } from "bun:test"

import type { Database } from "kern-db"
import type { AnyDb } from "kern-orm"
import type pg from "pg"
import type postgres from "postgres"

import type { RetryResult } from "../core/administration"
import type { EnqueueResult } from "../core/enqueue"
import { defineTask } from "../task/task"

import { createJobs } from "./jobs"

declare const jobs: ReturnType<typeof createJobs>

const sendEmail = defineTask("email.send").withPayload<{ to: string }>()
const purgeExpiredSessions = defineTask("sessions.purgeExpired")
const notify = defineTask("notify").withPayload<{ id: string } | undefined>()

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

  test("takes a scope, a function that returns one, null, or undefined", () => {
    const pool = {} as pg.Pool
    createJobs(pool, { scope: "tenant" })
    createJobs(pool, { scope: () => "tenant" })
    // @ts-expect-error The function returns a string.
    createJobs(pool, { scope: () => null })
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
