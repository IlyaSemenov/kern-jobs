// The SQL function `enqueue` of the schema: the same jobs and results as the enqueue of the core, its arguments, many rows of one statement, transactions and concurrent producers, and its independence of the search path and the name of the schema.
import { afterEach, beforeEach, describe, expect, test } from "bun:test"

import { waitForLock } from "kern-orm/testing"

import { migrateJobs } from "../migration/migrate"
import {
  type Conn,
  createTestSchema,
  isPending,
  query,
  type TestSchema,
} from "../test-harness/database"
import { createRandom } from "../test-harness/random"
import { claimPlain } from "../worker/db/claim"

import { enqueue, type EnqueueParams } from "./enqueue"
import type { JobTable } from "./job-table"
import { quoteIdentifier } from "./postgres"
import { queryValues, type ValuesTarget } from "./statement"

let schema: TestSchema
beforeEach(async () => {
  schema = await createTestSchema()
})
afterEach(async () => {
  await schema.dispose()
})

/** Types of the arguments of the SQL function by their names. */
const argTypes = {
  task: "text",
  payload: "jsonb",
  scope: "text",
  key: "text",
  queue: "text",
  key_mode: "text",
  run_at: "timestamptz",
  delay: "float8",
  max_wait: "float8",
  max_attempts: "int",
  reset_attempts: "boolean",
}

/** Arguments of the SQL function by their names, a payload as its JSON text; an absent one is omitted from the call. */
type SqlArgs = Partial<Record<keyof typeof argTypes, string | number | boolean | null>>

/** Result of a call of the SQL function, with the time of its statement as text. */
interface SqlResult {
  id: string
  created: boolean
  now: string
}

/** Calls the SQL function of a job table with the given arguments in named notation, and returns its result. */
async function callEnqueue(
  target: ValuesTarget,
  args: SqlArgs,
  table: JobTable = schema.table,
): Promise<SqlResult> {
  const names = Object.keys(args) as (keyof typeof argTypes)[]
  const list = names.map((name, i) => `${name} => $${i + 1}::text::${argTypes[name]}`).join(", ")
  const values = await queryValues(
    target,
    `select e.id::text, e.created::text, statement_timestamp()::text
     from ${quoteIdentifier(table.schemaName)}.enqueue(${list}) e`,
    names.map(name => args[name]),
  )
  const [id, created, now] = values[0]!
  return { id: id!, created: created === "true", now: now! }
}

/** Returns the rows of a scope in the order of their ids, with every column that an enqueue sets as text, other than the id and the scope. */
async function rowsOf(scope: string) {
  return query(
    schema.admin,
    `select task, queue, key, key_mode, payload::text as payload, run_at::text as run_at, not_before::text as not_before,
            window_started_at::text as window_started_at, created_at::text as created_at, attempts::text as attempts,
            max_attempts::text as max_attempts, state
     from ${schema.table.name} where scope = $1 order by id`,
    [scope],
  )
}

/** Returns the ids of the rows of a scope in their order. */
async function idsOf(scope: string): Promise<string[]> {
  const rows = await query(
    schema.admin,
    `select j.id::text as id from ${schema.table.name} j where scope = $1 order by j.id`,
    [scope],
  )
  return rows.map(row => row.id!)
}

describe("the same jobs as the enqueue of the core", () => {
  /** Options of an enqueue in a step, those of the core without the task, the scope, and the time. */
  type Options = Omit<EnqueueParams, "task" | "scope" | "now">

  /**
   * Step of a sequence that both scopes take:
   * - `enqueue`: an enqueue of task `t`, by the SQL function in scope `sql` and by the core in scope `core` at the time of the statement of the function;
   * - `wait`: moves every time of the rows back by its seconds, as the passing of that time;
   * - `claim`, `fail`, and `complete`: what a worker does to the job of a key, without a worker: a claim of its pending job, a return of its earliest active job to pending with `not_before` after its seconds, unless a pending job of the key exists, and a completion of its active job.
   */
  type Step =
    | { enqueue: Options }
    | { wait: number }
    | { claim: string }
    | { fail: string; seconds: number }
    | { complete: string }

  /** Returns the arguments of the SQL function for the options of the core, NULL for an absent option. */
  function toSqlArgs(o: Options): SqlArgs {
    return {
      task: "t",
      payload: o.payload === undefined ? null : JSON.stringify(o.payload),
      scope: "sql",
      key: o.key ?? null,
      queue: o.queue ?? null,
      key_mode: o.mode ?? null,
      run_at: o.runAt?.toISOString() ?? null,
      delay: o.delay ?? null,
      max_wait: o.maxWait ?? null,
      max_attempts: o.maxAttempts ?? null,
      reset_attempts: o.resetAttempts ?? null,
    }
  }

  /** Target of the statements of the core whose time, the `$1` parameter, is the given text, such as the time of a statement of the SQL function to the microsecond. */
  function atTime(now: string): ValuesTarget {
    return {
      execute: statement =>
        schema.admin.execute({ ...statement, parameters: [now, ...statement.parameters.slice(1)] }),
    }
  }

  /** Runs a step in both scopes and checks that they give the same result and the same rows. */
  async function runStep(step: Step): Promise<void> {
    const table = schema.table.name
    if ("enqueue" in step) {
      const sql = await callEnqueue(schema.admin, toSqlArgs(step.enqueue))
      const core = await enqueue(atTime(sql.now), schema.table, {
        task: "t",
        scope: "core",
        ...step.enqueue,
      })
      expect(sql.created).toBe(core.created)
      // The rows of the two scopes match by their order, so each result is the row of the same position.
      expect((await idsOf("sql")).indexOf(sql.id)).toBe((await idsOf("core")).indexOf(core.id))
    } else if ("wait" in step) {
      await query(
        schema.admin,
        `update ${table} set run_at = run_at - make_interval(secs => $1::text::float8),
           not_before = not_before - make_interval(secs => $1::text::float8),
           window_started_at = window_started_at - make_interval(secs => $1::text::float8),
           created_at = created_at - make_interval(secs => $1::text::float8)`,
        [step.wait],
      )
    } else if ("claim" in step) {
      await query(
        schema.admin,
        `update ${table} j set state = 'active', attempts = attempts + 1, claim_id = nextval('${schema.table.sequence}'),
           lease_until = statement_timestamp() + interval '1 hour', locked_by = 'w', locked_at = statement_timestamp()
         where key = $1::text and state = 'pending'
           and not exists (select from ${table} a where a.scope = j.scope and a.queue = j.queue and a.state = 'active')`,
        [step.claim],
      )
    } else if ("fail" in step) {
      await query(
        schema.admin,
        `update ${table} j set state = 'pending', claim_id = null, lease_until = null,
           not_before = statement_timestamp() + make_interval(secs => $2::text::float8),
           run_at = statement_timestamp() + make_interval(secs => $2::text::float8), window_started_at = statement_timestamp()
         where j.id = (select min(a.id) from ${table} a where a.scope = j.scope and a.key = $1::text and a.state = 'active')
           and not exists (select from ${table} p where p.scope = j.scope and p.task = j.task and p.key = j.key and p.state = 'pending')`,
        [step.fail, step.seconds],
      )
    } else {
      await query(schema.admin, `delete from ${table} where key = $1::text and state = 'active'`, [
        step.complete,
      ])
    }
    expect(await rowsOf("sql")).toEqual(await rowsOf("core"))
  }

  /** Runs the steps in their order, checking both scopes after each. */
  async function runSteps(steps: readonly Step[]): Promise<void> {
    for (const step of steps) await runStep(step)
    expect((await rowsOf("sql")).length).toBeGreaterThan(0)
  }

  /** Returns the time that is the given seconds from the clock of the test. */
  const fromNow = (seconds: number) => new Date(Date.now() + seconds * 1000)

  const debounce = { mode: "debounce", delay: 2, maxWait: 10 } as const

  test("inserts a job by each of its options", async () => {
    await runSteps([
      { enqueue: {} },
      { enqueue: { payload: null } },
      { enqueue: { payload: { a: [1, "x"] }, queue: "q", maxAttempts: 3, delay: 5 } },
      { enqueue: { key: "", runAt: fromNow(-30) } },
      { enqueue: { key: "k", mode: "keep", runAt: fromNow(60) } },
      // The first event of a debounce is due after the smaller of delay and maxWait.
      { enqueue: { key: "d", mode: "debounce", delay: 7, maxWait: 3 } },
    ])
  })

  test("replace takes the payload, the queue, the attempt budget, and the run time, not below the backoff, and keeps or resets the attempts", async () => {
    await runSteps([
      { enqueue: { key: "k", payload: 1, queue: "q", maxAttempts: 5 } },
      { claim: "k" },
      { fail: "k", seconds: 30 },
      { enqueue: { key: "k", payload: 2, queue: null, maxAttempts: 2, delay: 5 } },
      { enqueue: { key: "k", payload: 3, runAt: fromNow(60) } },
      { enqueue: { key: "k", payload: 4, resetAttempts: true } },
    ])
  })

  test("keep keeps the pending job and takes only its key mode", async () => {
    await runSteps([
      { enqueue: { key: "k", payload: 1, queue: "q", maxAttempts: 5, delay: 9 } },
      { claim: "k" },
      { fail: "k", seconds: 3 },
      { enqueue: { key: "k", mode: "keep", payload: 2, queue: "r", maxAttempts: 1, delay: 0 } },
      { enqueue: { key: "k", mode: "keep", resetAttempts: true } },
    ])
  })

  test("debounce postpones the job within its window, never earlier, and another key mode ends the window", async () => {
    await runSteps([
      { enqueue: { key: "k", ...debounce } },
      { wait: 1.5 },
      { enqueue: { key: "k", ...debounce, payload: 2 } },
      // A smaller delay or maxWait does not make the job due earlier.
      { wait: 1 },
      { enqueue: { key: "k", ...debounce, delay: 0, maxWait: 0 } },
      { wait: 6 },
      // The delay would put the run time past the end of the window.
      { enqueue: { key: "k", ...debounce } },
      { enqueue: { key: "k", ...debounce, maxWait: 30 } },
      { enqueue: { key: "k", mode: "replace", delay: 1 } },
      { wait: 5 },
      { enqueue: { key: "k", ...debounce } },
      { enqueue: { key: "k", mode: "keep" } },
      { enqueue: { key: "k", ...debounce, resetAttempts: true } },
    ])
  })

  test("debounce after a failure keeps the window that the failure opened and the run time at the end of its backoff", async () => {
    await runSteps([
      { enqueue: { key: "k", ...debounce } },
      { claim: "k" },
      { fail: "k", seconds: 30 },
      { enqueue: { key: "k", ...debounce } },
      { wait: 4 },
      { enqueue: { key: "k", ...debounce, maxWait: 60 } },
    ])
  })

  test("an enqueue while the job of the key is active inserts the next pending job", async () => {
    await runSteps([
      { enqueue: { key: "k", payload: 1 } },
      { claim: "k" },
      { enqueue: { key: "k", mode: "keep", payload: 2 } },
      { enqueue: { key: "k", mode: "keep", payload: 3 } },
      { complete: "k" },
    ])
  })

  for (const seed of [1, 2, 3]) {
    test(`random sequences of enqueues and of the steps of a worker, seed ${seed}`, async () => {
      const random = createRandom(seed)
      const pick = <T>(items: readonly T[]): T => items[random.int(0, items.length - 1)]!
      const keys = ["a", "b", null] as const
      for (let i = 0; i < 120; i++) {
        const kind = random.float()
        const key = pick(["a", "b"])
        if (kind < 0.08) await runStep({ wait: pick([0.5, 1, 2.5, 8]) })
        else if (kind < 0.16) await runStep({ claim: key })
        else if (kind < 0.24) await runStep({ fail: key, seconds: pick([0, 3, 15]) })
        else if (kind < 0.3) await runStep({ complete: key })
        else {
          const mode = pick(["replace", "keep", "debounce", undefined] as const)
          const time =
            mode === "debounce"
              ? { delay: pick([0, 0.5, 2, 10]), maxWait: pick([0, 1, 5, 10]) }
              : pick([{}, { delay: pick([0, 1, 5]) }, { runAt: fromNow(pick([-5, 0, 3, 20])) }])
          await runStep({
            enqueue: {
              key: pick(keys),
              mode,
              ...time,
              payload: pick([undefined, null, { i }]),
              queue: pick([undefined, null, "q1", "q2"]),
              maxAttempts: pick([undefined, 1, 4]),
              resetAttempts: pick([undefined, false, true]),
            },
          })
        }
      }
    })
  }
})

describe("arguments", () => {
  test("an argument that is NULL takes its default, as an omitted one: no payload, key, or queue, replace, the time of the statement, and 25 attempts", async () => {
    const omitted = await callEnqueue(schema.admin, { task: "t" })
    const nulls = await callEnqueue(schema.admin, {
      task: "t",
      payload: null,
      key: null,
      queue: null,
      key_mode: null,
      run_at: null,
      delay: null,
      max_wait: null,
      max_attempts: null,
      reset_attempts: null,
    })
    expect([omitted.created, nulls.created]).toEqual([true, true])
    const rows = await query(
      schema.admin,
      `select j.id::text as id, scope, (payload is null)::text as no_payload, key, queue, key_mode,
              max_attempts::text as max_attempts, (run_at = window_started_at and run_at = created_at)::text as at_now
       from ${schema.table.name} j order by j.id`,
    )
    const defaults = {
      scope: "",
      no_payload: "true",
      key: null,
      queue: null,
      key_mode: "replace",
      max_attempts: "25",
      at_now: "true",
    }
    expect(rows).toEqual([
      { id: omitted.id, ...defaults },
      { id: nulls.id, ...defaults },
    ])
  })

  test("takes the bounds of the options of the jobs API: delay and max_wait of 1e12 seconds, run_at in the years 1 and 9999, and max_attempts of 2^31 − 1", async () => {
    const t = { task: "t" }
    await callEnqueue(schema.admin, {
      ...t,
      run_at: "0001-01-01T00:00:00Z",
      max_attempts: 2 ** 31 - 1,
    })
    await callEnqueue(schema.admin, { ...t, run_at: "9999-12-31T23:59:59.999999Z" })
    await callEnqueue(schema.admin, { ...t, delay: 1e12 })
    await callEnqueue(schema.admin, { ...t, key_mode: "debounce", delay: 1e12, max_wait: 1e12 })
    // Outside debounce, max_wait is ignored.
    await callEnqueue(schema.admin, { ...t, key_mode: "replace", max_wait: -1 })
    expect(
      (await query(schema.admin, `select count(*)::text as n from ${schema.table.name}`))[0]!.n,
    ).toBe("5")
  })

  test("rejects a wrong argument with its error, and the table rejects a value over its limit", async () => {
    const t = { task: "t" }
    const debounce = { key_mode: "debounce", delay: 2, max_wait: 10 }
    const cases: [SqlArgs, string, string][] = [
      [{ task: null }, "22004", "task must not be null"],
      [
        { task: "t:failure" },
        "22023",
        "The name of a task must not end with :failure, which names the failure task of a task, got t:failure",
      ],
      [{ ...t, scope: null }, "22004", "scope must not be null: omit it for an unscoped job"],
      [
        { ...t, key_mode: "merge" },
        "22023",
        "key_mode must be replace, keep, or debounce, got merge",
      ],
      [
        { ...t, delay: 1, run_at: "2030-01-01T00:00:00Z" },
        "22023",
        "Pass either delay or run_at, not both",
      ],
      [
        { ...debounce, ...t, delay: null },
        "22023",
        "key_mode debounce takes delay and max_wait, and no run_at",
      ],
      [
        { ...debounce, ...t, max_wait: null },
        "22023",
        "key_mode debounce takes delay and max_wait, and no run_at",
      ],
      [
        { ...t, key_mode: "debounce", max_wait: 10, run_at: "2030-01-01T00:00:00Z" },
        "22023",
        "key_mode debounce takes delay and max_wait, and no run_at",
      ],
      [{ ...t, delay: -1 }, "22023", "delay must be a number of seconds from 0 to 1e12, got -1"],
      [{ ...t, delay: 1e13 }, "22023", "delay must be a number of seconds from 0 to 1e12"],
      [
        { ...t, delay: "NaN" },
        "22023",
        "delay must be a number of seconds from 0 to 1e12, got NaN",
      ],
      [{ ...t, delay: "Infinity" }, "22023", "delay must be a number of seconds from 0 to 1e12"],
      [
        { ...debounce, ...t, max_wait: -1 },
        "22023",
        "max_wait must be a number of seconds from 0 to 1e12, got -1",
      ],
      [
        { ...t, run_at: "infinity" },
        "22023",
        "run_at must be a time in the years 1 to 9999, got infinity",
      ],
      [{ ...t, run_at: "-infinity" }, "22023", "run_at must be a time in the years 1 to 9999"],
      [
        { ...t, run_at: "10000-01-01T00:00:00Z" },
        "22023",
        "run_at must be a time in the years 1 to 9999",
      ],
      [
        { ...t, max_attempts: 0 },
        "22023",
        "max_attempts must be an integer from 1 to 2147483647, got 0",
      ],
      [{ task: "t".repeat(129) }, "23514", 'violates check constraint "job_task_check"'],
      [{ ...t, scope: "s".repeat(129) }, "23514", 'violates check constraint "job_scope_check"'],
      [{ ...t, key: "k".repeat(1025) }, "23514", 'violates check constraint "job_key_check"'],
      [{ ...t, queue: "q".repeat(1025) }, "23514", 'violates check constraint "job_queue_check"'],
    ]
    for (const [args, code, message] of cases) {
      const outcome = callEnqueue(schema.admin, args)
      await expect(outcome).rejects.toMatchObject({ code })
      await expect(outcome).rejects.toThrow(message)
    }
    const count = await query(schema.admin, `select count(*)::text as n from ${schema.table.name}`)
    expect(count[0]!.n).toBe("0")
  })
})

describe("many rows of one statement", () => {
  test("enqueues a job for each row, merging the rows of one key one after another, as separate enqueues", async () => {
    const results = await query(
      schema.admin,
      `select e.id::text as id, e.created::text as created
       from (values (1, 'a'), (2, 'a'), (3, null), (4, 'b'), (5, 'a')) v(n, k)
       cross join lateral ${quoteIdentifier(schema.name)}.enqueue(task => 't', key => v.k, payload => to_jsonb(v.n)) e
       order by v.n`,
    )
    const [a, b] = [results[0]!.id, results[3]!.id]
    expect(results.map(r => r.created)).toEqual(["true", "false", "true", "true", "false"])
    expect(results.map(r => r.id)).toEqual([a, a, results[2]!.id, b, a])
    const rows = await query(
      schema.admin,
      `select key, payload::text as payload from ${schema.table.name} order by id`,
    )
    expect(rows).toEqual([
      { key: "a", payload: "5" },
      { key: null, payload: "3" },
      { key: "b", payload: "4" },
    ])
  })

  test("every row takes the time of the statement that calls the function, not of its transaction, so the debounce of repeated keys keeps the run time of the first", async () => {
    const producer = await schema.connect()
    await query(producer, "begin")
    await query(producer, "select pg_sleep(0.01)")
    const [call] = await query(
      producer,
      `select statement_timestamp()::text as now, transaction_timestamp()::text as began, count(*)::text as n
       from (
         select ${quoteIdentifier(schema.name)}.enqueue(task => 't', key => v.k, key_mode => 'debounce', delay => 2, max_wait => 10)
         from (values ('a'), ('a'), ('b')) v(k)
       ) e`,
    )
    await query(producer, "commit")
    expect(call!.n).toBe("3")
    expect(call!.now).not.toBe(call!.began)
    const rows = await query(
      schema.admin,
      `select key, created_at::text as created_at, (run_at - created_at)::text as wait
       from ${schema.table.name} order by id`,
    )
    expect(rows).toEqual([
      { key: "a", created_at: call!.now!, wait: "00:00:02" },
      { key: "b", created_at: call!.now!, wait: "00:00:02" },
    ])
  })
})

describe("transactions", () => {
  const W = { worker: "w1", tasks: ["t"], limit: 10, now: null }

  test("the job is invisible to workers until the transaction of the caller commits, and a rollback removes it", async () => {
    const producer = await schema.connect()
    await query(producer, "begin")
    await callEnqueue(producer, { task: "t", key: "rolled-back" })
    expect(await claimPlain(schema.admin, schema.table, W)).toEqual([])
    await query(producer, "rollback")
    await query(producer, "begin")
    const { id } = await callEnqueue(producer, { task: "t", key: "committed" })
    expect(await claimPlain(schema.admin, schema.table, W)).toEqual([])
    await query(producer, "commit")
    expect((await claimPlain(schema.admin, schema.table, W)).map(claim => claim.id)).toEqual([id])
  })

  for (const outcome of ["commit", "rollback"] as const) {
    test(`a producer of a key waits for an uncommitted enqueue of the key, and after its ${outcome} ${outcome === "commit" ? "merges into the job" : "inserts its own"}`, async () => {
      const [first, second] = [await schema.connect(), await schema.connect()]
      await query(first, "begin")
      const a = await callEnqueue(first, { task: "t", key: "k", payload: "1" })
      const event = callEnqueue(second, { task: "t", key: "k", payload: "2" })
      await waitForLock(schema.admin, second.pid)
      expect(await isPending(event)).toBe(true)
      await query(first, outcome)
      const b = await event
      expect(b.created).toBe(outcome === "rollback")
      expect(b.id === a.id).toBe(outcome === "commit")
      const rows = await query(
        schema.admin,
        `select payload::text as payload from ${schema.table.name}`,
      )
      expect(rows).toEqual([{ payload: "2" }])
    })
  }

  test("keep holds the pending job until the transaction of the caller ends, so a claim skips it", async () => {
    const { id } = await callEnqueue(schema.admin, { task: "t", key: "k" })
    const producer = await schema.connect()
    await query(producer, "begin")
    expect(await callEnqueue(producer, { task: "t", key: "k", key_mode: "keep" })).toMatchObject({
      id,
      created: false,
    })
    expect(await claimPlain(schema.admin, schema.table, W)).toEqual([])
    await query(producer, "commit")
    expect((await claimPlain(schema.admin, schema.table, W)).map(claim => claim.id)).toEqual([id])
  })
})

describe("schema", () => {
  /** Migrates the schema of kern-jobs of a name that needs quotes, beside the schema of the test. */
  async function migrateQuoted(): Promise<JobTable> {
    const table = schema.unmigratedTable('Jobs "x"')
    await migrateJobs(schema.driver, { schemaName: table.schemaName })
    return table
  }

  test("enqueues into its own schema, whatever the search path of the caller and its temporary table of the same name", async () => {
    const table = await migrateQuoted()
    const caller: Conn = await schema.connect()
    await query(caller, `set search_path = pg_temp, ${quoteIdentifier(schema.name)}`)
    await query(caller, "create temporary table job (id bigint, task text)")
    const { id } = await callEnqueue(caller, { task: "t", scope: "tenant" }, table)
    const rows = await query(
      schema.admin,
      `select 'quoted' as source, id::text as id, scope from ${table.name}
       union all select 'test', id::text, scope from ${schema.table.name}`,
    )
    expect(rows).toEqual([{ source: "quoted", id, scope: "tenant" }])
  })

  test("checks its arguments by the operators of PostgreSQL, whatever operators the search path of the caller gives", async () => {
    const ops = quoteIdentifier(schema.unmigratedTable("ops").schemaName)
    await query(schema.admin, `create schema ${ops}`)
    await query(
      schema.admin,
      `create function ${ops}.accept(float8, int) returns boolean language sql immutable return true`,
    )
    await query(
      schema.admin,
      `create operator ${ops}.>= (leftarg = float8, rightarg = int, function = ${ops}.accept)`,
    )
    const caller = await schema.connect()
    await query(caller, `set search_path = ${ops}, pg_catalog`)
    const outcome = callEnqueue(caller, { task: "t", delay: -1 })
    await expect(outcome).rejects.toMatchObject({ code: "22023" })
  })

  test("keeps enqueueing into its table after the schema is renamed", async () => {
    const table = await migrateQuoted()
    const renamed = schema.unmigratedTable("renamed")
    await query(
      schema.admin,
      `alter schema ${quoteIdentifier(table.schemaName)} rename to ${quoteIdentifier(renamed.schemaName)}`,
    )
    const { id } = await callEnqueue(schema.admin, { task: "t" }, renamed)
    const rows = await query(schema.admin, `select id::text as id from ${renamed.name}`)
    expect(rows).toEqual([{ id }])
  })
})
