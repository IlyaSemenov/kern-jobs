// The view `job_summary`: its columns, and the summary of the jobs of each task in each scope.
import { afterEach, beforeEach, expect, test } from "bun:test"

import { createTestSchema, query, type TestSchema } from "../test-harness/database"

import { quoteIdentifier } from "./postgres"
import type { TextRow } from "./statement"

let schema: TestSchema
beforeEach(async () => {
  schema = await createTestSchema()
})
afterEach(() => schema.dispose())

test("has the columns of its contract", async () => {
  const rows = await query(
    schema.admin,
    `select column_name, data_type from information_schema.columns
      where table_schema = $1 and table_name = 'job_summary' order by ordinal_position`,
    [schema.name],
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
  const [{ reference }] = (await query(
    schema.admin,
    "select statement_timestamp()::text as reference",
  )) as [{ reference: string }]
  const at = (seconds: number) => `$1::timestamptz + make_interval(secs => ${seconds})`
  const columns =
    "scope, task, state, attempts, max_attempts, run_at, not_before, window_started_at, created_at, claim_id, locked_at, lease_until, failed_at"
  const pending = (
    scope: string,
    task: string,
    attempts: number,
    runAt: number,
    notBefore?: number,
  ) =>
    `('${scope}', '${task}', 'pending', ${attempts}, 10, ${at(runAt)}, ${notBefore === undefined ? "null" : at(notBefore)}, ${at(-1000)}, ${at(-1000)}, null, null, null, null)`
  const active = (
    task: string,
    attempts: number,
    claimId: number,
    lockedAt: number,
    leaseUntil: number,
  ) =>
    `('', '${task}', 'active', ${attempts}, 10, ${at(-300)}, null, ${at(-1000)}, ${at(-1000)}, ${claimId}, ${at(lockedAt)}, ${at(leaseUntil)}, null)`
  const failed = (task: string, failedAt: number) =>
    `('', '${task}', 'failed', 7, 7, ${at(-1000)}, null, ${at(-1000)}, ${at(-1000)}, null, null, null, ${at(failedAt)})`
  await query(
    schema.admin,
    `insert into ${schema.table.name} (${columns}) values ${[
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
    ].join(", ")}`,
    [reference],
  )
  // The select runs in a transaction that began earlier, whose start is not the time of the statement, and the job of task "turning" becomes due between them.
  await schema.admin.executeScript(`begin;
    insert into ${schema.table.name} (${columns}) values ('', 'turning', 'pending', 1, 10,
      transaction_timestamp() + interval '50 milliseconds', transaction_timestamp() + interval '50 milliseconds',
      transaction_timestamp(), transaction_timestamp(), null, null, null, null);
    select pg_sleep(0.1)`)
  const rows = await query(
    schema.admin,
    `select extract(epoch from statement_timestamp() - $1::timestamptz)::float8 as elapsed, s.*
      from ${quoteIdentifier(schema.name)}.job_summary s order by scope, task`,
    [reference],
  )
  await schema.admin.executeScript("commit")
  const summaries = rows.map(row => {
    const { elapsed, ...summary } = row
    // An age less the time since the reference, rounded, since the times of the rows and of the statement are of microseconds.
    const toAge = (name: string) => {
      expect(summary).toHaveProperty(name)
      const text = summary[name] ?? null
      return text === null ? null : Math.round((Number(text) - Number(elapsed)) * 1000) / 1000
    }
    return {
      ...summary,
      oldest_due_seconds: toAge("oldest_due_seconds"),
      longest_running_seconds: toAge("longest_running_seconds"),
      oldest_failed_seconds: toAge("oldest_failed_seconds"),
    } as Record<string, TextRow[string] | number>
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
      most_attempts: "4",
    },
    { ...empty, scope: "", task: "b", failed: "1", oldest_failed_seconds: 40, most_attempts: null },
    { ...empty, scope: "", task: "debounced", scheduled: "1", most_attempts: "1" },
    { ...empty, scope: "", task: "reset", retrying: "1", most_attempts: "0" },
    {
      ...empty,
      scope: "",
      task: "turning",
      due: "1",
      oldest_due_seconds: expect.any(Number),
      most_attempts: "1",
    },
    { ...empty, scope: "tenant", task: "a", scheduled: "1", most_attempts: "0" },
  ])
})
