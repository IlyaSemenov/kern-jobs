// The view `job_summary`: its columns, and the summary of the jobs of each task in each scope.

import { afterEach, beforeEach, expect, test } from "bun:test"

import { identifier, joinSql, param, sql, type SqlFragment } from "kern-orm/db"

import { createTestSchema, type TestSchema } from "../test-harness/database"
import { workerSchema } from "../worker/db/db"
import * as t from "../worker/db/types"

let schema: TestSchema
beforeEach(async () => {
  schema = await createTestSchema()
})
afterEach(() => schema.dispose())

test("has the columns of its contract", async () => {
  const rows = await schema.admin.query`
    select column_name, data_type from information_schema.columns
    where table_schema = ${schema.name} and table_name = 'job_summary' order by ordinal_position`.select(
    {
      column_name: t.text,
      data_type: t.text,
    },
  )
  expect(rows.map(row => [row.column_name, row.data_type])).toEqual([
    ["scope", "text"],
    ["task", "text"],
    ["due", "bigint"],
    ["scheduled", "bigint"],
    ["retrying", "bigint"],
    ["running", "bigint"],
    ["failed", "bigint"],
    ["oldest_due_seconds", "double precision"],
    ["longest_running_seconds", "double precision"],
    ["oldest_failed_seconds", "double precision"],
    ["most_attempts", "integer"],
  ])
})

test("summarizes the jobs of each task in each scope by the time of the statement that reads it", async () => {
  // The rows take their times relative to a reference of the clock of the database.
  const reference = param(
    t.timestamptz,
    await schema.admin.query`select statement_timestamp()::text`.get(t.text),
  )
  const at = (seconds: number) =>
    sql`${reference} + make_interval(secs => ${param(t.doublePrecision, seconds)})`
  const pending = (
    scope: string,
    task: string,
    attempts: number,
    runAt: number,
    notBefore?: number,
  ) =>
    sql`(${scope}, ${task}, 'pending', ${attempts}, 10, ${at(runAt)}, ${notBefore === undefined ? sql`null` : at(notBefore)}, ${at(-1000)}, ${at(-1000)}, null, null, null, null)`
  const active = (
    task: string,
    attempts: number,
    claimId: number,
    lockedAt: number,
    leaseUntil: number,
  ) =>
    sql`('', ${task}, 'active', ${attempts}, 10, ${at(-300)}, null, ${at(-1000)}, ${at(-1000)}, ${claimId}, ${at(lockedAt)}, ${at(leaseUntil)}, null)`
  const failed = (task: string, failedAt: number) =>
    sql`('', ${task}, 'failed', 7, 7, ${at(-1000)}, null, ${at(-1000)}, ${at(-1000)}, null, null, null, ${at(failedAt)})`
  const values: SqlFragment[] = [
    // Due: a first attempt, and a retry whose backoff has ended.
    pending("", "a", 0, -10),
    pending("", "a", 2, -30, -30),
    pending("", "a", 0, 3600),
    // Running: the longest runs from its claim, not from its run time, though a heartbeat extended the lease of the other past its own.
    active("a", 4, 1, -100, 10),
    active("a", 1, 2, -20, 60),
    // Failed jobs spent more attempts than any pending or active one.
    failed("a", -500),
    failed("a", -50),
    failed("b", -40),
    // A debounce window of a job that failed before, after its backoff, is scheduled.
    pending("", "debounced", 1, 3600, -5),
    // A job whose attempts an enqueue reset is retrying while its backoff lasts.
    pending("", "reset", 0, 120, 60),
    pending("tenant", "a", 0, 3600),
  ]
  await schema.admin.query`
    insert into ${workerSchema.job}
      (scope, task, state, attempts, max_attempts, run_at, not_before, window_started_at, created_at, claim_id, locked_at, lease_until, failed_at)
    values ${joinSql(values)}`
  // The select runs in a transaction that began earlier, whose start is not the time of the statement, and the job of task "turning" becomes due between them.
  await schema.admin.query`begin`
  await schema.admin.query`
    insert into ${workerSchema.job} (scope, task, state, attempts, max_attempts, run_at, not_before, window_started_at, created_at, claim_id, locked_at, lease_until, failed_at)
    values ('', 'turning', 'pending', 1, 10,
      transaction_timestamp() + interval '50 milliseconds', transaction_timestamp() + interval '50 milliseconds',
      transaction_timestamp(), transaction_timestamp(), null, null, null, null)`
  await schema.admin.query`select pg_sleep(0.1)`
  const rows = await schema.admin.query`
    select extract(epoch from statement_timestamp() - ${reference})::float8 as elapsed, s.*
    from ${identifier(schema.name, "job_summary")} s order by scope, task`.select({
    elapsed: t.doublePrecision,
    scope: t.text,
    task: t.text,
    due: t.bigintText,
    scheduled: t.bigintText,
    retrying: t.bigintText,
    running: t.bigintText,
    failed: t.bigintText,
    oldest_due_seconds: t.doublePrecision.nullable(),
    longest_running_seconds: t.doublePrecision.nullable(),
    oldest_failed_seconds: t.doublePrecision.nullable(),
    most_attempts: t.integer.nullable(),
  })
  await schema.admin.query`commit`
  const summaries = rows.map(row => {
    const { elapsed, ...summary } = row
    // An age less the time since the reference, rounded, since the times of the rows and of the statement are of microseconds.
    const toAge = (seconds: number | null) =>
      seconds === null ? null : Math.round((seconds - elapsed) * 1000) / 1000
    return {
      ...summary,
      oldest_due_seconds: toAge(summary.oldest_due_seconds),
      longest_running_seconds: toAge(summary.longest_running_seconds),
      oldest_failed_seconds: toAge(summary.oldest_failed_seconds),
    }
  })
  const empty = {
    due: "0",
    scheduled: "0",
    retrying: "0",
    running: "0",
    failed: "0",
    oldest_due_seconds: null,
    longest_running_seconds: null,
    oldest_failed_seconds: null,
  }
  expect(summaries).toEqual([
    {
      scope: "",
      task: "a",
      due: "2",
      scheduled: "1",
      retrying: "0",
      running: "2",
      failed: "2",
      oldest_due_seconds: 30,
      longest_running_seconds: 100,
      oldest_failed_seconds: 500,
      most_attempts: 4,
    },
    { ...empty, scope: "", task: "b", failed: "1", oldest_failed_seconds: 40, most_attempts: null },
    { ...empty, scope: "", task: "debounced", scheduled: "1", most_attempts: 1 },
    { ...empty, scope: "", task: "reset", retrying: "1", most_attempts: 0 },
    {
      ...empty,
      scope: "",
      task: "turning",
      due: "1",
      oldest_due_seconds: expect.any(Number),
      most_attempts: 1,
    },
    { ...empty, scope: "tenant", task: "a", scheduled: "1", most_attempts: 0 },
  ])
})
