// The SQL function `retry` of the schema: the same jobs and results as the retry of the core, its waits for concurrent transactions, the failed jobs of a query in one statement, and its independence of the search path.

import { afterEach, beforeEach, describe, expect, test } from "bun:test"

import { identifier } from "kern-orm/db"
import { waitForLock } from "kern-orm/testing"

import { retryFailedJob } from "../producer/administration"
import {
  atTime,
  type Conn,
  createTestSchema,
  idsOf,
  jobs,
  only,
  T,
  type TestSchema,
} from "../test-harness/database"
import { claimCycle, claimPlain } from "../worker/db/claim"
import { schema } from "../worker/db/db"
import { fail } from "../worker/db/settlement"
import * as t from "../worker/db/types"
import { isUniqueViolation } from "../worker/db/violation"

import { enqueue, enqueueItems, type EnqueueParams } from "./enqueue"

let testSchema: TestSchema
beforeEach(async () => {
  testSchema = await createTestSchema()
})
afterEach(async () => {
  await testSchema.dispose()
})

/** Result of a call of the SQL function, with the time of its statement as text. */
interface SqlResult {
  retried: boolean
  pendingId: string | null
  now: string
}

/** Calls the SQL function of the test schema with an id on a connection, and returns its row, or `null` without one. */
async function callRetry(conn: Conn, id: string): Promise<SqlResult | null> {
  const rows = await conn.query`
    select r.retried, r.pending_id::text as pending_id, statement_timestamp()::text as now
    from ${identifier(testSchema.name, "retry")}(${id}::bigint) r`.select({
    retried: t.boolean,
    pending_id: t.text.nullable(),
    now: t.text,
  })
  return rows.length === 0
    ? null
    : { retried: rows[0]!.retried, pendingId: rows[0]!.pending_id, now: rows[0]!.now }
}

/** Claims the ready job of task `t` at the given time on the connection of the test. */
async function claimAt(seconds: number) {
  return only(
    await claimCycle(testSchema.conn.query, {
      worker: "w1",
      tasks: ["t"],
      slots: 1,
      now: T(seconds),
    }),
  )
}

/**
 * Enqueues a job of task `t` in a scope at T(0), claims it at T(1), fails it for good at T(2), and returns its id.
 * A claim takes the earliest ready job of task `t` of any scope, so no other job of the task may be ready.
 */
async function createFailedJob(
  scope: string,
  o: Partial<EnqueueParams> & { maxItems?: number } = {},
): Promise<string> {
  const { maxItems, ...params } = { task: "t", scope, payload: "failed", now: T(0), ...o }
  const { id } =
    maxItems === undefined
      ? await enqueue(testSchema.conn, testSchema.table, params)
      : only(await enqueueItems(testSchema.conn, testSchema.table, { ...params, maxItems }))
  const failed = await fail(testSchema.db, await claimAt(1), {
    error: "boom",
    reason: "terminal-error",
    backoff: 0,
    now: T(2),
  })
  expect(failed).toBe("failed")
  return id
}

/**
 * Returns the rows of a scope in the order of their ids, with every column other than the id and the scope, its JSON and times as text to compare them exactly.
 * The claim id is a value of the sequence of the table, which gives each claim of either scope a value of its own, so only whether a row has one is compared.
 */
async function rowsOf(scope: string) {
  return testSchema.conn.query`
    select task, queue, key, key_mode, payload::text as payload, run_at::text as run_at, not_before::text as not_before,
           window_started_at::text as window_started_at, created_at::text as created_at, attempts, max_attempts, state,
           claim_id is not null as claimed, lease_until::text as lease_until, locked_by, locked_at::text as locked_at,
           last_error, failed_at::text as failed_at, failure_max_attempts, max_items, sealed
    from ${schema.job} where scope = ${scope} order by id`.select({
    task: t.text,
    queue: t.text.nullable(),
    key: t.text.nullable(),
    key_mode: t.text,
    payload: t.text.nullable(),
    run_at: t.text,
    not_before: t.text.nullable(),
    window_started_at: t.text,
    created_at: t.text,
    attempts: t.integer,
    max_attempts: t.integer,
    state: t.text,
    claimed: t.boolean,
    lease_until: t.text.nullable(),
    locked_by: t.text.nullable(),
    locked_at: t.text.nullable(),
    last_error: t.text.nullable(),
    failed_at: t.text.nullable(),
    failure_max_attempts: t.integer.nullable(),
    max_items: t.integer.nullable(),
    sealed: t.boolean,
  })
}

describe("the same jobs and results as the retry of the core", () => {
  /** Builds the same jobs in scopes `sql` and `core` with `setup`, which takes the scope. */
  async function setupBoth(setup: (scope: string) => Promise<void>): Promise<void> {
    for (const scope of ["sql", "core"]) await setup(scope)
  }

  /**
   * Retries the job at the given position of each scope, by the SQL function in `sql` and by the core in `core` at the time of the statement of the function, and checks that they give the same result and the same rows, with the same ids in each scope.
   * Returns the result of the SQL function.
   */
  async function compareRetry(position: number): Promise<SqlResult | null> {
    const sqlIds = await idsOf(testSchema.conn.query, "sql")
    const coreIds = await idsOf(testSchema.conn.query, "core")
    expect(sqlIds).toHaveLength(coreIds.length)
    const called = await callRetry(testSchema.conn, sqlIds[position]!)
    const target = called ? atTime(testSchema.conn, called.now) : testSchema.conn
    const core = await retryFailedJob(target, testSchema.table, { id: coreIds[position]! })
    if (!called) expect(core).toBeNull()
    else if (called.retried) expect(core).toEqual({ retried: true })
    else {
      // The rows of the two scopes match by their order, so the pending job is the row of the same position.
      const pendingId = coreIds[sqlIds.indexOf(called.pendingId!)]!
      expect(core).toEqual({ retried: false, pendingId })
    }
    expect(await idsOf(testSchema.conn.query, "sql")).toEqual(sqlIds)
    expect(await idsOf(testSchema.conn.query, "core")).toEqual(coreIds)
    expect(await rowsOf("sql")).toEqual(await rowsOf("core"))
    return called
  }

  test("returns a failed job to pending in place, with a full budget of attempts and no backoff, and keeps its other columns", async () => {
    // The job fails with a backoff before it fails for good, so that it has a backoff to reset.
    await setupBoth(async scope => {
      const o = { task: "t", scope, key: "k", queue: "q", mode: "keep", maxAttempts: 2 } as const
      await enqueue(testSchema.conn, testSchema.table, { ...o, payload: 1, now: T(0) })
      await fail(testSchema.db, await claimAt(1), { error: "first", backoff: 3, now: T(2) })
      await fail(testSchema.db, await claimAt(5), { error: "boom", backoff: 3, now: T(6) })
    })
    expect(await rowsOf("sql")).toMatchObject([
      { state: "failed", attempts: 2, not_before: expect.any(String) },
    ])
    expect(await compareRetry(0)).toMatchObject({ retried: true, pendingId: null })
    expect(await rowsOf("sql")).toMatchObject([
      { state: "pending", attempts: 0, not_before: null, failed_at: null, last_error: "boom" },
    ])
  })

  test("a pending job of the key keeps the failed job from returning, returns the id of the pending job, and both stay as they are", async () => {
    await setupBoth(async scope => {
      await createFailedJob(scope, { key: "k" })
      await enqueue(testSchema.conn, testSchema.table, {
        task: "t",
        scope,
        key: "k",
        payload: "pending",
        now: T(3),
      })
    })
    const pendingId = (await idsOf(testSchema.conn.query, "sql"))[1]
    expect(await compareRetry(0)).toMatchObject({ retried: false, pendingId })
    expect((await rowsOf("sql")).map(r => r.state)).toEqual(["failed", "pending"])
  })

  test("returns a failed batch to pending sealed beside the open batch of its key", async () => {
    await setupBoth(async scope => {
      const batch = { task: "t", scope, key: "", maxItems: 3 }
      await createFailedJob(scope, { ...batch, payload: [1] })
      await enqueueItems(testSchema.conn, testSchema.table, { ...batch, payload: [2], now: T(3) })
    })
    expect(await compareRetry(0)).toMatchObject({ retried: true, pendingId: null })
    expect((await rowsOf("sql")).map(r => [r.state, r.payload, r.sealed])).toEqual([
      ["pending", "[1]", true],
      ["pending", "[2]", false],
    ])
  })

  test("returns no row and changes nothing for an active or pending id", async () => {
    await setupBoth(async scope => {
      await enqueue(testSchema.conn, testSchema.table, { task: "t", scope, now: T(0) })
      await claimAt(1)
      await enqueue(testSchema.conn, testSchema.table, { task: "t", scope, now: T(10) })
    })
    expect((await rowsOf("sql")).map(r => r.state)).toEqual(["active", "pending"])
    const before = await jobs(testSchema.conn.query)
    expect(await compareRetry(0)).toBeNull()
    expect(await compareRetry(1)).toBeNull()
    expect(await jobs(testSchema.conn.query)).toEqual(before)
  })

  test("returns no row for an unknown id", async () => {
    expect(await callRetry(testSchema.conn, "999999")).toBeNull()
  })
})

describe("concurrent transactions", () => {
  for (const end of ["commit", "rollback"] as const) {
    test(`waits for an uncommitted insert of a pending job of the key; producer ${end}`, async () => {
      const failed = await createFailedJob("", { key: "k" })
      const producer = await testSchema.connect()
      const operator = await testSchema.connect()
      await producer.query`begin`
      const { id: pending } = await enqueue(producer, testSchema.table, {
        task: "t",
        key: "k",
        now: T(12),
      })
      // The rejection is handled before the commit is awaited, since the operator can receive the error before the producer receives the response to its commit.
      const retried = callRetry(operator, failed).then(
        result => ({ result }),
        (error: unknown) => ({ error }),
      )
      await waitForLock(testSchema.conn, operator.pid)
      await (end === "commit" ? producer.query`commit` : producer.query`rollback`)
      const outcome = await retried
      if (end === "commit") {
        // The snapshot of the retry missed the pending job, so the unique index rejects the update, which changes nothing.
        expect("error" in outcome && isUniqueViolation(outcome.error, "job_open_key_idx")).toBe(
          true,
        )
        expect((await jobs(testSchema.conn.query)).map(r => [r.id, r.state])).toEqual([
          [failed, "failed"],
          [pending, "pending"],
        ])
      } else {
        expect(outcome).toMatchObject({ result: { retried: true, pendingId: null } })
        expect((await jobs(testSchema.conn.query)).map(r => [r.id, r.state])).toEqual([
          [failed, "pending"],
        ])
      }
    })
  }

  test("waits for an uncommitted claim of the pending job of the key, and then returns the failed job to pending with its key", async () => {
    const failed = await createFailedJob("", { key: "k" })
    const { id: claimed } = await enqueue(testSchema.conn, testSchema.table, {
      task: "t",
      key: "k",
      now: T(12),
    })
    const worker = await testSchema.connect()
    const operator = await testSchema.connect()
    await worker.query`begin`
    only(await claimPlain(worker.query, { worker: "w1", tasks: ["t"], limit: 1, now: T(13) }))
    const retried = callRetry(operator, failed)
    await waitForLock(testSchema.conn, operator.pid)
    await worker.query`commit`
    expect(await retried).toMatchObject({ retried: true, pendingId: null })
    expect((await jobs(testSchema.conn.query)).map(r => [r.id, r.state, r.key])).toEqual([
      [failed, "pending", "k"],
      [claimed, "active", "k"],
    ])
  })

  test("of two concurrent retries of a job, the second waits for the first and returns no row", async () => {
    const failed = await createFailedJob("")
    const operator1 = await testSchema.connect()
    const operator2 = await testSchema.connect()
    await operator1.query`begin`
    expect(await callRetry(operator1, failed)).toMatchObject({ retried: true })
    const retried = callRetry(operator2, failed)
    await waitForLock(testSchema.conn, operator2.pid)
    await operator1.query`commit`
    expect(await retried).toBeNull()
    expect((await jobs(testSchema.conn.query)).map(r => [r.id, r.state])).toEqual([
      [failed, "pending"],
    ])
  })
})

test("retries the failed jobs of a query one after another, so of the failed jobs of a key, one returns and the other finds it pending", async () => {
  const keyed = [await createFailedJob("", { key: "k" }), await createFailedJob("", { key: "k" })]
  const unkeyed = await createFailedJob("")
  const results = await testSchema.conn.query`
    select j.id, r.retried, r.pending_id::text as pending_id
    from ${schema.job} j cross join lateral ${identifier(testSchema.name, "retry")}(j.id) r
    where j.state = 'failed'
    order by j.id`.select({ id: t.bigintText, retried: t.boolean, pending_id: t.text.nullable() })
  // The plan of the query decides which of the jobs of the key the function retries first.
  const winner = results.find(r => keyed.includes(r.id) && r.retried)!.id
  const loser = keyed.find(id => id !== winner)!
  expect(results).toEqual([
    ...keyed.map(id =>
      id === winner
        ? { id, retried: true, pending_id: null }
        : { id, retried: false, pending_id: winner },
    ),
    { id: unkeyed, retried: true, pending_id: null },
  ])
  expect((await jobs(testSchema.conn.query)).map(r => [r.id, r.state])).toEqual(
    [...keyed, unkeyed].map(id => [id, id === loser ? "failed" : "pending"]),
  )
})

test("retries a job of its own schema, whatever the search path of the caller and its temporary table of the same name", async () => {
  const id = await createFailedJob("tenant")
  const caller = await testSchema.connect()
  await caller.query`set search_path = pg_temp`
  await caller.query`create temporary table job (id bigint, state text, scope text, task text, key text)`
  await caller.query`insert into pg_temp.job values (${id}::bigint, 'failed', 'tenant', 't', null)`
  expect(await callRetry(caller, id)).toMatchObject({ retried: true, pendingId: null })
  expect((await rowsOf("tenant")).map(r => r.state)).toEqual(["pending"])
})
