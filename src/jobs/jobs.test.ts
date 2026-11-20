// Enqueues, retries, and deletes through the clients of an application: node-postgres and postgres.js, with their transactions, renamed columns, and parsers of their own, and a Database of kern-db and a Db of kern-orm.
import { afterEach, beforeEach, describe, expect, test } from "bun:test"
import { runInNewContext } from "node:vm"

import { createDatabase } from "kern-db"
import { postgresJsDriver } from "kern-db/postgres-js"
import { bindSchema, defineSchema } from "kern-orm"
import pg from "pg"
import postgres from "postgres"

import { type JobsClient, query } from "../core/statement"
import {
  createTestSchema,
  databaseUrl,
  jobs as jobRows,
  only,
  type TestSchema,
} from "../core/testing"
import { defineTask } from "../task/task"

import {
  ALL_SCOPES,
  createJobs,
  type DeleteFailedFilter,
  type DeletePendingFilter,
  type JobsOptions,
} from "./jobs"

/** Client of an application whose enqueues follow the transaction that `transaction` holds. */
interface Client {
  /** Client that `createJobs` takes. */
  jobsClient: JobsClient
  /**
   * Runs `fn` with the client of a transaction, which `createJobs` takes; the transaction commits when `fn` resolves and rolls back when it rejects.
   */
  transaction(fn: (client: JobsClient) => Promise<void>): Promise<void>
  close(): Promise<void>
}

/** Parser of a client that fails a test when it runs: the enqueue must read only text. */
const failParse = (oid: unknown) => () => {
  throw new Error(`the parser of type ${oid} ran`)
}

/** Creates a client over a pg pool, whose transactions run on a `PoolClient`. */
function createPgClient(config: pg.PoolConfig = {}): Client {
  const pool = new pg.Pool({ connectionString: databaseUrl(), max: 2, ...config })
  let current: pg.PoolClient | undefined
  return {
    jobsClient: () => current ?? pool,
    async transaction(fn) {
      const client = await pool.connect()
      try {
        await client.query("begin")
        current = client
        await fn(client)
        await client.query("commit")
      } catch (error) {
        await client.query("rollback")
        throw error
      } finally {
        current = undefined
        client.release()
      }
    },
    close: () => pool.end(),
  }
}

/** Creates a client over a postgres.js pool, whose transactions run in `sql.begin`. */
function createPostgresJsClient(
  options: postgres.Options<Record<string, postgres.PostgresType>> = {},
): Client {
  const sql = postgres(databaseUrl(), { max: 2, onnotice: () => {}, ...options })
  let current: postgres.TransactionSql | undefined
  return {
    jobsClient: () => current ?? sql,
    async transaction(fn) {
      await sql.begin(async tx => {
        current = tx
        try {
          await fn(tx)
        } finally {
          current = undefined
        }
      })
    },
    close: () => sql.end(),
  }
}

/** Creates a `Database` of kern-db, whose enqueues join its transactions, given as it is or as a `Db` of kern-orm. */
function createKernClient(kind: "database" | "db"): Client {
  const database = createDatabase({
    driver: postgresJsDriver(postgres(databaseUrl(), { max: 2, onnotice: () => {} })),
  })
  return {
    jobsClient: kind === "database" ? database : bindSchema(defineSchema(), database),
    // The database joins its current transaction.
    transaction: fn => database.transaction(() => fn(database)),
    close: () => database.close(),
  }
}

const clients: [string, () => Client][] = [
  ["pg", () => createPgClient()],
  [
    "pg with binary results and parsers of its own",
    () =>
      createPgClient({
        binary: true,
        types: {
          getTypeParser: (oid: number, format?: string) =>
            oid === pg.types.builtins.TEXT
              ? pg.types.getTypeParser(oid, format as "text")
              : failParse(oid),
        },
      } as pg.PoolConfig),
  ],
  ["postgres.js", () => createPostgresJsClient()],
  [
    "postgres.js with postgres.camel and parsers of its own",
    () =>
      createPostgresJsClient({
        transform: postgres.camel,
        types: {
          failing: { from: [16, 20, 114, 1184, 3802], parse: failParse("non-text") },
        },
      }),
  ],
  ["Database of kern-db", () => createKernClient("database")],
  ["Db of kern-orm", () => createKernClient("db")],
]

const sendEmail = defineTask("email.send").withPayload<{ to: string }>()
const indexProduct = defineTask("search.indexProduct")
  .withPayload<{ productId: string; version: number }>()
  .withOptions(({ productId }) => ({ key: `product:${productId}`, keyMode: "keep" }))

let schema: TestSchema
beforeEach(async () => {
  schema = await createTestSchema()
})
afterEach(async () => {
  await schema.dispose()
})

/** Marks a pending job of the test schema failed `age` seconds ago, as a worker does after the last attempt of the job. */
const markFailed = (id: string, age = 0) =>
  query(
    schema.admin,
    `update ${schema.table.name} set state = 'failed', attempts = 1, last_error = 'boom',
       failed_at = now() - make_interval(secs => $2::text::float8)
     where id = $1::text::bigint`,
    [id, age],
  )

for (const [name, createClient] of clients) {
  describe(name, () => {
    let client: Client
    beforeEach(() => {
      client = createClient()
    })
    afterEach(async () => {
      await client.close()
    })

    const createTestJobs = () => createJobs(client.jobsClient, { schemaName: schema.name })

    test("enqueues a job outside a transaction", async () => {
      const result = await createTestJobs().enqueue(sendEmail, { to: "a@example.com" })
      const row = only(await jobRows(schema))
      expect(result).toEqual({ id: row.id, created: true })
      expect([row.task, row.payload]).toEqual(["email.send", { to: "a@example.com" }])
    })

    test("a job enqueued in a transaction appears when it commits", async () => {
      const jobs = createTestJobs()
      await client.transaction(async () => {
        await jobs.enqueue(sendEmail, { to: "a@example.com" })
        expect(await jobRows(schema)).toEqual([])
      })
      expect(await jobRows(schema)).toHaveLength(1)
    })

    test("enqueues in a transaction through the client of the transaction", async () => {
      await client.transaction(async tx => {
        await createJobs(tx, { schemaName: schema.name }).enqueue(sendEmail, {
          to: "a@example.com",
        })
        expect(await jobRows(schema)).toEqual([])
      })
      expect(await jobRows(schema)).toHaveLength(1)
    })

    test("a job enqueued in a transaction that rolls back never appears", async () => {
      const jobs = createTestJobs()
      const error = new Error("rollback")
      await expect(
        client.transaction(async () => {
          await jobs.enqueue(sendEmail, { to: "a@example.com" })
          throw error
        }),
      ).rejects.toBe(error)
      expect(await jobRows(schema)).toEqual([])
    })

    test("merges an enqueue into the pending job of its key", async () => {
      const jobs = createTestJobs()
      const first = await jobs.enqueue(indexProduct, { productId: "42", version: 1 })
      const second = await jobs.enqueue(indexProduct, { productId: "42", version: 2 })
      expect(second).toEqual({ id: first.id, created: false })
      expect(only(await jobRows(schema)).payload).toEqual({ productId: "42", version: 1 })
    })

    test("retries a failed job, reports a pending job of its key that keeps it from returning, and deletes a failed or pending job", async () => {
      const jobs = createTestJobs()
      const { id } = await jobs.enqueue(indexProduct, { productId: "42", version: 1 })
      await markFailed(id)
      expect(await jobs.retry(id)).toEqual({ retried: true })
      expect(only(await jobRows(schema)).state).toBe("pending")
      await markFailed(id)
      const { id: pending } = await jobs.enqueue(indexProduct, { productId: "42", version: 2 })
      expect(await jobs.retry(id)).toEqual({ retried: false, pendingId: pending })
      expect([await jobs.delete(id), await jobs.delete(pending)]).toEqual([true, true])
      expect([await jobs.retry(id), await jobs.delete(id)]).toEqual([null, false])
      expect(await jobRows(schema)).toEqual([])
    })

    test("a retry and deletes in a transaction that rolls back change nothing", async () => {
      const jobs = createTestJobs()
      const { id: retried } = await jobs.enqueue(sendEmail, { to: "a@example.com" })
      const { id: deleted } = await jobs.enqueue(sendEmail, { to: "b@example.com" })
      const { id: old } = await jobs.enqueue(sendEmail, { to: "c@example.com" })
      await jobs.enqueue(indexProduct, { productId: "42", version: 1 })
      await markFailed(retried)
      await markFailed(old, 60)
      const before = await jobRows(schema)
      const error = new Error("rollback")
      await expect(
        client.transaction(async () => {
          expect(await jobs.retry(retried)).toEqual({ retried: true })
          expect(await jobs.delete(deleted)).toBe(true)
          expect(await jobs.deletePending({ task: indexProduct })).toBe(1)
          expect(await jobs.deleteFailed({ task: sendEmail, olderThan: 30 })).toBe(1)
          throw error
        }),
      ).rejects.toBe(error)
      expect(await jobRows(schema)).toEqual(before)
    })
  })
}

test("throws TypeError for a client of another kind, such as a connection string", () => {
  expect(() => createJobs(databaseUrl() as unknown as JobsClient)).toThrow(
    new TypeError(
      "client must be a pg.Pool, a client of postgres.js, an executor, a function that returns one of them, a Database of kern-db, or a Db of kern-orm",
    ),
  )
})

test("throws TypeError for a schemaName that checkSchemaName rejects, such as null or one of 64 bytes", () => {
  const executor = { query: () => Promise.reject(new Error("a statement ran")) }
  for (const schemaName of [null, "", 5, "a".repeat(64)]) {
    const create = () => createJobs(executor, { schemaName } as unknown as JobsOptions)
    expect(create).toThrow(TypeError)
    expect(create).toThrow("schemaName must be")
  }
})

test("retry and delete reject an id that is not a string with TypeError, and one outside the ids of the table with RangeError, before they call the client", async () => {
  let calls = 0
  const jobs = createJobs(() => {
    calls++
    return { query: () => Promise.reject(new Error("a statement ran")) }
  })
  for (const method of ["retry", "delete"] as const) {
    await expect(jobs[method](42 as unknown as string)).rejects.toThrow(
      new TypeError("id must be a string, got 42"),
    )
    for (const id of ["", "0", "01", "-1", "1.5", " 1", "abc", "9223372036854775808"]) {
      await expect(jobs[method](id)).rejects.toThrow(
        new RangeError(`id must be a decimal integer from 1 to 9223372036854775807, got "${id}"`),
      )
    }
  }
  expect(calls).toBe(0)
  // The largest id of the table reaches the client.
  await expect(jobs.retry("9223372036854775807")).rejects.toThrow("a statement ran")
})

test("deletePending rejects an argument that is not an object of a task and, optionally, its key, a task name that is not storable text, and a key that is neither true nor storable text, with TypeError, before it calls the client", async () => {
  let calls = 0
  const jobs = createJobs(() => {
    calls++
    return { query: () => Promise.reject(new Error("a statement ran")) }
  })
  // A task defined by an older version of kern-jobs, whose defineTask accepted such a name.
  const olderTask = { ...sendEmail, name: "t\ud800" }
  await expect(jobs.deletePending({ task: olderTask })).rejects.toThrow(TypeError)
  // An explicit undefined too, which would otherwise widen the delete to every key of the task.
  for (const key of [undefined, 42, null, false, "k\u0000", "k\udc00"]) {
    await expect(jobs.deletePending({ task: sendEmail, key: key as string })).rejects.toThrow(
      "key must be true or a string that has",
    )
  }
  // As JavaScript passes them, past the types: each would otherwise delete more jobs than it names.
  for (const filter of [
    undefined,
    sendEmail,
    "k",
    null,
    [],
    {},
    { key: "k" },
    { task: "email.send" },
    { scope: "a" },
    Object.create({ scope: "a" }),
    { task: sendEmail, keys: ["k"] },
    { [Symbol("task")]: sendEmail },
    Object.defineProperty({}, "scope", { value: "a" }),
    Promise.resolve({ task: sendEmail, key: "k" }),
    new Map([["task", sendEmail]]),
    new (class Tenant {
      readonly #id = "a"
      get id() {
        return this.#id
      }
    })(),
  ]) {
    await expect(jobs.deletePending(filter as DeletePendingFilter)).rejects.toThrow(
      new TypeError("deletePending takes a filter of a task and, optionally, its key"),
    )
  }
  expect(calls).toBe(0)
})

test("deletePending takes a filter of another realm, with a null prototype, or with inherited getters", async () => {
  const values: unknown[][] = []
  const jobs = createJobs({
    query: async (_text: string, params: unknown[]) => {
      values.push(params)
      return { rows: [["0"]] }
    },
  })
  await jobs.deletePending(runInNewContext("({ task })", { task: sendEmail }))
  await jobs.deletePending(Object.assign(Object.create(null), { task: sendEmail, key: "k" }))
  // The getters are inherited from the prototype of the class, and each is read once.
  const reads = { task: 0, key: 0 }
  await jobs.deletePending(
    new (class {
      get task() {
        reads.task++
        return sendEmail
      }
      get key() {
        reads.key++
        return "k"
      }
    })(),
  )
  expect(values).toEqual([
    ["", "email.send"],
    ["", "email.send", "k"],
    ["", "email.send", "k"],
  ])
  expect(reads).toEqual({ task: 1, key: 1 })
})

test("deletePending reads the name of the task once, so that the statement gets the name that it checked", async () => {
  const values: unknown[][] = []
  const jobs = createJobs(
    {
      query: async (_text: string, params: unknown[]) => {
        values.push(params)
        return { rows: [["0"]] }
      },
    },
    { scope: "a" },
  )
  let reads = 0
  const task = {
    ...defineTask("search.indexProduct", { scoped: true }),
    get name() {
      return reads++ === 0 ? "search.indexProduct" : "t\ud800"
    },
  }
  expect(await jobs.deletePending({ task, key: "k" })).toBe(0)
  expect(values).toEqual([["a", "search.indexProduct", "k"]])
})

test("deletePending reads the task and its name before the key", async () => {
  const values: unknown[][] = []
  const jobs = createJobs({
    query: async (_text: string, params: unknown[]) => {
      values.push(params)
      return { rows: [["0"]] }
    },
  })
  const filter: Record<string, unknown> = {
    task: {
      get name() {
        filter.key = "k"
        return "t"
      },
    },
  }
  await jobs.deletePending(filter as unknown as DeletePendingFilter)
  expect(values).toEqual([["", "t", "k"]])
  const invalidTask = {
    task: null,
    get key(): string {
      throw new Error("the key was read")
    },
  }
  await expect(jobs.deletePending(invalidTask as unknown as DeletePendingFilter)).rejects.toThrow(
    new TypeError("deletePending takes a filter of a task and, optionally, its key"),
  )
})

test("deleteFailed reads the task before it checks for olderThan", async () => {
  const values: unknown[][] = []
  const jobs = createJobs({
    query: async (_text: string, params: unknown[]) => {
      values.push(params)
      return { rows: [["0"]] }
    },
  })
  const filter: { task: typeof sendEmail; olderThan?: number } = {
    get task() {
      filter.olderThan = 3600
      return sendEmail
    },
  }
  await jobs.deleteFailed(filter)
  expect(values).toEqual([[null, "3600", "", "email.send"]])
})

test("deleteFailed rejects an argument that is not an object of a task, olderThan, or both, and a task name that is not storable text, with TypeError, and olderThan that is not a number of seconds from 0 to 1e12 with RangeError, before it calls the client", async () => {
  let calls = 0
  const jobs = createJobs(() => {
    calls++
    return { query: () => Promise.reject(new Error("a statement ran")) }
  })
  await expect(jobs.deleteFailed({ task: { ...sendEmail, name: "t\ud800" } })).rejects.toThrow(
    "The name of a task must be a string that has",
  )
  // An explicit undefined too, which would otherwise widen the delete to every age.
  for (const olderThan of [undefined, null, "60", -1, NaN, Infinity, 1e12 + 1]) {
    await expect(
      jobs.deleteFailed({ task: sendEmail, olderThan: olderThan as number }),
    ).rejects.toThrow(
      new RangeError(
        `olderThan must be a number of seconds from 0 to 1e12, got ${String(olderThan)}`,
      ),
    )
  }
  // As JavaScript passes them, past the types: each would otherwise delete more jobs than it names.
  for (const filter of [
    undefined,
    sendEmail,
    60,
    null,
    [],
    {},
    { task: undefined },
    { task: "email.send" },
    { scope: "a" },
    { olderThan: 60, before: 60 },
    Object.create({ scope: "a" }),
    { [Symbol("olderThan")]: 60 },
    Object.defineProperty({ olderThan: 60 }, "scope", { value: "a" }),
    Promise.resolve({ olderThan: 60 }),
  ]) {
    await expect(jobs.deleteFailed(filter as DeleteFailedFilter)).rejects.toThrow(
      new TypeError("deleteFailed takes a filter of a task, olderThan, or both"),
    )
  }
  // A field that only the prototype of every object has is not a field of a filter without a prototype.
  Object.defineProperty(Object.prototype, "olderThan", { value: 0, configurable: true })
  try {
    await expect(jobs.deleteFailed(Object.create(null))).rejects.toThrow(
      new TypeError("deleteFailed takes a filter of a task, olderThan, or both"),
    )
  } finally {
    delete (Object.prototype as { olderThan?: unknown }).olderThan
  }
  expect(calls).toBe(0)
})

test("deleteFailed takes a filter of another realm, with a null prototype, or with inherited getters, each read once", async () => {
  const values: unknown[][] = []
  const jobs = createJobs({
    query: async (_text: string, params: unknown[]) => {
      values.push(params)
      return { rows: [["0"]] }
    },
  })
  await jobs.deleteFailed(runInNewContext("({ olderThan: 60 })"))
  await jobs.deleteFailed(Object.assign(Object.create(null), { task: sendEmail }))
  const reads = { task: 0, olderThan: 0 }
  await jobs.deleteFailed(
    new (class {
      get task() {
        reads.task++
        return sendEmail
      }
      get olderThan() {
        reads.olderThan++
        return 0.5
      }
    })(),
  )
  expect(values).toEqual([
    [null, "60", ""],
    ["", "email.send"],
    [null, "0.5", "", "email.send"],
  ])
  expect(reads).toEqual({ task: 1, olderThan: 1 })
})

describe("scope", () => {
  let pool: pg.Pool
  beforeEach(() => {
    pool = new pg.Pool({ connectionString: databaseUrl(), max: 2 })
  })
  afterEach(async () => {
    await pool.end()
  })

  const indexProduct = defineTask("search.indexProduct", { scoped: true })

  /** Returns the scopes of the jobs of the test schema in the order of their ids. */
  const scopes = async () => (await jobRows(schema)).map(row => row.scope)

  test("writes the scope of createJobs, a string or what its function returns at the enqueue, to each job of a scoped task", async () => {
    // 128 bytes in UTF-8, the longest scope.
    const longest = "é".repeat(64)
    await createJobs(pool, { schemaName: schema.name, scope: longest }).enqueue(indexProduct)
    let tenant = "a"
    const jobs = createJobs(pool, { schemaName: schema.name, scope: () => tenant })
    await jobs.enqueue(indexProduct)
    tenant = "b"
    await jobs.enqueue(indexProduct)
    expect(await scopes()).toEqual([longest, "a", "b"])
  })

  test("enqueues a job of a task that is not scoped without a scope, and does not call the function", async () => {
    let calls = 0
    const tenant = () => {
      calls++
      return "a"
    }
    for (const scope of ["a", tenant, ALL_SCOPES, null, undefined] as JobsOptions["scope"][]) {
      await createJobs(pool, { schemaName: schema.name, scope }).enqueue(sendEmail, { to: "a" })
    }
    expect([await scopes(), calls]).toEqual([["", "", "", "", ""], 0])
  })

  test("rejects an enqueue of a scoped task without a scope, with ALL_SCOPES, or with an invalid one that its function returns, before it calls the client", async () => {
    let calls = 0
    const enqueueIn = (scope: JobsOptions["scope"]) => {
      const client = () => {
        calls++
        return pool
      }
      return createJobs(client, { schemaName: schema.name, scope }).enqueue(indexProduct)
    }
    for (const scope of [undefined, null]) {
      await expect(enqueueIn(scope)).rejects.toThrow(
        new TypeError("Task search.indexProduct is scoped: pass its scope to createJobs"),
      )
    }
    await expect(enqueueIn(ALL_SCOPES)).rejects.toThrow(
      new TypeError(
        "Task search.indexProduct is scoped: pass createJobs the scope of its jobs rather than ALL_SCOPES",
      ),
    )
    // As a JavaScript function returns it, past the types.
    await expect(enqueueIn(() => undefined as unknown as string)).rejects.toThrow("scope must be")
    await expect(enqueueIn(() => "")).rejects.toThrow(
      new RangeError('scope must be 1 to 128 bytes in UTF-8, got ""'),
    )
    expect(calls).toBe(0)
  })

  test("retries and deletes only the jobs of the scope of createJobs, a string or what its function returns at the call, the jobs of every scope with ALL_SCOPES, and only the jobs without a scope when createJobs has none", async () => {
    const enqueueIn = (scope: string) =>
      createJobs(pool, { schemaName: schema.name, scope }).enqueue(indexProduct)
    const { id: failed } = await enqueueIn("a")
    const { id: pending } = await enqueueIn("b")
    await markFailed(failed)
    const withoutScope = createJobs(pool, { schemaName: schema.name })
    const { id: unscoped } = await withoutScope.enqueue(sendEmail, { to: "a" })
    expect([await withoutScope.retry(failed), await withoutScope.delete(pending)]).toEqual([
      null,
      false,
    ])
    let tenant = "b"
    const jobs = createJobs(pool, { schemaName: schema.name, scope: () => tenant })
    expect([await jobs.retry(failed), await jobs.delete(failed)]).toEqual([null, false])
    tenant = "a"
    expect(await jobs.retry(failed)).toEqual({ retried: true })
    expect(await createJobs(pool, { schemaName: schema.name, scope: "a" }).delete(pending)).toBe(
      false,
    )
    expect([await jobs.delete(unscoped), await withoutScope.delete(unscoped)]).toEqual([
      false,
      true,
    ])
    const everyScope = createJobs(pool, { schemaName: schema.name, scope: ALL_SCOPES })
    await markFailed(failed)
    expect(await everyScope.retry(failed)).toEqual({ retried: true })
    expect([await everyScope.delete(failed), await everyScope.delete(pending)]).toEqual([
      true,
      true,
    ])
  })

  test("rejects a retry, a delete, and a delete of pending or failed jobs without a task whose scope function throws or returns an invalid scope, also ALL_SCOPES, before it calls the client", async () => {
    let calls = 0
    const jobsIn = (scope: () => string) =>
      createJobs(
        () => {
          calls++
          return pool
        },
        { schemaName: schema.name, scope },
      )
    const error = new Error("no request")
    const outsideRequest = jobsIn(() => {
      throw error
    })
    await expect(outsideRequest.retry("1")).rejects.toBe(error)
    await expect(outsideRequest.delete("1")).rejects.toBe(error)
    await expect(outsideRequest.deletePending()).rejects.toBe(error)
    await expect(outsideRequest.deleteFailed()).rejects.toBe(error)
    await expect(outsideRequest.deleteFailed({ olderThan: 0 })).rejects.toBe(error)
    await expect(jobsIn(() => "").delete("1")).rejects.toThrow(
      new RangeError('scope must be 1 to 128 bytes in UTF-8, got ""'),
    )
    // As a JavaScript function returns it, past the types: ALL_SCOPES is an option of createJobs only.
    await expect(jobsIn(() => ALL_SCOPES as unknown as string).delete("1")).rejects.toThrow(
      "scope must be a string that has",
    )
    expect(calls).toBe(0)
  })

  test("deletes the pending jobs of a scoped task, or of a key of it, in the scope of createJobs, and of a task that is not scoped in the empty scope", async () => {
    let tenant = "a"
    const jobs = createJobs(pool, { schemaName: schema.name, scope: () => tenant })
    await jobs.enqueue(indexProduct, undefined, { key: "k" })
    tenant = "b"
    await jobs.enqueue(indexProduct, undefined, { key: "k" })
    await jobs.enqueue(indexProduct)
    await jobs.enqueue(sendEmail, { to: "a" }, { key: "k" })
    await jobs.enqueue(sendEmail, { to: "a" })
    expect(await jobs.deletePending({ task: indexProduct, key: "k" })).toBe(1)
    expect(await scopes()).toEqual(["a", "b", "", ""])
    expect(await jobs.deletePending({ task: indexProduct })).toBe(1)
    expect(await jobs.deletePending({ task: sendEmail, key: "k" })).toBe(1)
    expect(await jobs.deletePending({ task: sendEmail, key: "k" })).toBe(0)
    expect(await jobs.deletePending({ task: sendEmail })).toBe(1)
    expect(await scopes()).toEqual(["a"])
  })

  test("deletes the pending jobs of a task that is not scoped without calling the function of the scope", async () => {
    const withoutScope = createJobs(pool, { schemaName: schema.name })
    await withoutScope.enqueue(sendEmail, { to: "a" }, { key: "k" })
    await withoutScope.enqueue(sendEmail, { to: "a" })
    const outsideRequest = createJobs(pool, {
      schemaName: schema.name,
      scope: () => {
        throw new Error("no request")
      },
    })
    expect(await outsideRequest.deletePending({ task: sendEmail, key: "k" })).toBe(1)
    expect(await outsideRequest.deletePending({ task: sendEmail })).toBe(1)
  })

  test("deletes the pending jobs of every task in the scope of createJobs, a string or what its function returns at the call, in every scope with ALL_SCOPES, and only the jobs without a scope when createJobs has none", async () => {
    const enqueueIn = (scope: string) =>
      createJobs(pool, { schemaName: schema.name, scope }).enqueue(indexProduct)
    await enqueueIn("a")
    await enqueueIn("b")
    await enqueueIn("b")
    const withoutScope = createJobs(pool, { schemaName: schema.name })
    await withoutScope.enqueue(sendEmail, { to: "a" })
    await enqueueIn("c")
    let tenant = "b"
    const jobs = createJobs(pool, { schemaName: schema.name, scope: () => tenant })
    expect(await jobs.deletePending()).toBe(2)
    expect(await scopes()).toEqual(["a", "", "c"])
    tenant = "d"
    expect(await jobs.deletePending()).toBe(0)
    expect(await withoutScope.deletePending()).toBe(1)
    expect(await scopes()).toEqual(["a", "c"])
    const everyScope = createJobs(pool, { schemaName: schema.name, scope: ALL_SCOPES })
    expect(await everyScope.deletePending()).toBe(2)
    expect(await scopes()).toEqual([])
  })

  test("deletes the pending jobs of a scoped task, or of a key of it, in every scope with ALL_SCOPES, and of a task that is not scoped in the empty scope", async () => {
    for (const scope of ["a", "b"]) {
      const jobs = createJobs(pool, { schemaName: schema.name, scope })
      await jobs.enqueue(indexProduct, undefined, { key: "k" })
      await jobs.enqueue(indexProduct, undefined, { key: "other" })
    }
    await createJobs(pool, { schemaName: schema.name }).enqueue(sendEmail, { to: "a" })
    // A job of an earlier definition of the task that was scoped.
    await createJobs(pool, { schemaName: schema.name, scope: "a" }).enqueue(
      defineTask("email.send", { scoped: true }),
    )
    const everyScope = createJobs(pool, { schemaName: schema.name, scope: ALL_SCOPES })
    expect(await everyScope.deletePending({ task: indexProduct, key: "k" })).toBe(2)
    expect(await everyScope.deletePending({ task: indexProduct })).toBe(2)
    expect(await everyScope.deletePending({ task: sendEmail })).toBe(1)
    expect(await scopes()).toEqual(["a"])
  })

  test("deletes the failed jobs of every task in the scope of createJobs, a string or what its function returns at the call, in every scope with ALL_SCOPES, and only the jobs without a scope when createJobs has none", async () => {
    const failIn = async (scope: string, age = 0) => {
      const { id } = await createJobs(pool, { schemaName: schema.name, scope }).enqueue(
        indexProduct,
      )
      await markFailed(id, age)
    }
    await failIn("a")
    await failIn("b", 60)
    await failIn("b")
    const withoutScope = createJobs(pool, { schemaName: schema.name })
    await markFailed((await withoutScope.enqueue(sendEmail, { to: "a" })).id)
    await failIn("c")
    let tenant = "b"
    const jobs = createJobs(pool, { schemaName: schema.name, scope: () => tenant })
    expect(await jobs.deleteFailed({ olderThan: 30 })).toBe(1)
    expect(await jobs.deleteFailed()).toBe(1)
    expect(await scopes()).toEqual(["a", "", "c"])
    tenant = "d"
    expect(await jobs.deleteFailed()).toBe(0)
    expect(await withoutScope.deleteFailed()).toBe(1)
    expect(await scopes()).toEqual(["a", "c"])
    const everyScope = createJobs(pool, { schemaName: schema.name, scope: ALL_SCOPES })
    expect(await everyScope.deleteFailed()).toBe(2)
    expect(await scopes()).toEqual([])
  })

  test("deletes the failed jobs of a scoped task in the scope of createJobs or in every scope with ALL_SCOPES, and of a task that is not scoped in the empty scope without calling the function of the scope", async () => {
    const jobsIn = (scope?: JobsOptions["scope"]) =>
      createJobs(pool, { schemaName: schema.name, scope })
    for (const scope of ["a", "b"]) await markFailed((await jobsIn(scope).enqueue(indexProduct)).id)
    await markFailed((await jobsIn().enqueue(sendEmail, { to: "a" })).id)
    // A job of an earlier definition of the task that was scoped.
    await markFailed((await jobsIn("a").enqueue(defineTask("email.send", { scoped: true }))).id)
    const outsideRequest = jobsIn(() => {
      throw new Error("no request")
    })
    expect(await outsideRequest.deleteFailed({ task: sendEmail })).toBe(1)
    expect(await jobsIn("b").deleteFailed({ task: indexProduct })).toBe(1)
    const everyScope = jobsIn(ALL_SCOPES)
    expect(await everyScope.deleteFailed({ task: indexProduct })).toBe(1)
    expect(await everyScope.deleteFailed({ task: sendEmail })).toBe(0)
    expect(await scopes()).toEqual(["a"])
  })

  test("rejects a delete of the pending or failed jobs of a scoped task without a scope, before it calls the client", async () => {
    let calls = 0
    const jobs = createJobs(
      () => {
        calls++
        return pool
      },
      { schemaName: schema.name },
    )
    const error = new TypeError("Task search.indexProduct is scoped: pass its scope to createJobs")
    for (const filter of [{ task: indexProduct }, { task: indexProduct, key: "k" }]) {
      await expect(jobs.deletePending(filter)).rejects.toThrow(error)
    }
    for (const filter of [{ task: indexProduct }, { task: indexProduct, olderThan: 0 }]) {
      await expect(jobs.deleteFailed(filter)).rejects.toThrow(error)
    }
    expect(calls).toBe(0)
  })

  test("ALL_SCOPES is the registered symbol kern-jobs.ALL_SCOPES, so that every copy of the package takes it", () => {
    expect<symbol>(ALL_SCOPES).toBe(Symbol.for("kern-jobs.ALL_SCOPES"))
  })

  test("createJobs rejects a scope of a wrong type, one that is not storable text, an empty one, or one longer than 128 bytes in UTF-8", () => {
    const executor = { query: () => Promise.reject(new Error("a statement ran")) }
    const create = (scope: unknown) => () =>
      createJobs(executor, { scope } as unknown as JobsOptions)
    for (const scope of [42, Symbol("kern-jobs.ALL_SCOPES")]) {
      expect(create(scope)).toThrow(
        new TypeError("scope must be a string, a function that returns one, or ALL_SCOPES"),
      )
    }
    expect(create("tenant\ud800")).toThrow("scope must be")
    expect(create("tenant\0")).toThrow("scope must be")
    for (const scope of ["", "é".repeat(64) + "a"]) {
      expect(create(scope)).toThrow(
        new RangeError(`scope must be 1 to 128 bytes in UTF-8, got "${scope}"`),
      )
    }
  })
})

describe("options", () => {
  let pool: pg.Pool
  beforeEach(() => {
    pool = new pg.Pool({ connectionString: databaseUrl(), max: 2 })
  })
  afterEach(async () => {
    await pool.end()
  })

  test("writes the options of defineTask, replaced by those of withOptions and then by the overrides of the enqueue", async () => {
    const task = defineTask("t", { queue: "q", maxAttempts: 3, delay: 5 })
      .withPayload<{ id: string }>()
      // A field given as undefined keeps the option of defineTask.
      .withOptions(({ id }) => ({ key: `k:${id}`, maxAttempts: id === "1" ? undefined : 4 }))
    const jobs = createJobs(pool, { schemaName: schema.name })
    await jobs.enqueue(task, { id: "1" })
    await jobs.enqueue(task, { id: "2" }, { key: null, delay: 0 })
    const [first, second] = await jobRows(schema)
    expect(first).toMatchObject({ key: "k:1", queue: "q", max_attempts: 3 })
    expect(first!.run_at.getTime() - first!.created_at.getTime()).toBe(5000)
    expect(second).toMatchObject({ key: null, queue: "q", max_attempts: 4 })
    expect(second!.run_at).toEqual(second!.created_at)
  })

  test("key true merges the enqueues of the task in each scope under the empty key, which deletePending takes too", async () => {
    const task = defineTask("t", { scoped: true, key: true }).withPayload<number>()
    const other = defineTask("other", { scoped: true, key: true })
    const jobs = createJobs(pool, { schemaName: schema.name, scope: "a" })
    const first = await jobs.enqueue(task, 1)
    expect(await jobs.enqueue(task, 2)).toEqual({ id: first.id, created: false })
    await jobs.enqueue(other)
    await createJobs(pool, { schemaName: schema.name, scope: "b" }).enqueue(task, 3)
    await jobs.enqueue(task, 4, { key: "k" })
    const rows = await jobRows(schema)
    expect(rows.map(({ task, scope, key, payload }) => ({ task, scope, key, payload }))).toEqual([
      { task: "t", scope: "a", key: "", payload: 2 },
      { task: "other", scope: "a", key: "", payload: undefined },
      { task: "t", scope: "b", key: "", payload: 3 },
      { task: "t", scope: "a", key: "k", payload: 4 },
    ])
    expect(await jobs.deletePending({ task, key: true })).toBe(1)
    expect((await jobRows(schema)).map(row => row.payload)).toEqual([undefined, 3, 4])
  })

  test("debounces by the delay and maxWait of the task", async () => {
    const task = defineTask("t", { key: "k", keyMode: "debounce", delay: 2, maxWait: 10 })
    const jobs = createJobs(pool, { schemaName: schema.name })
    await jobs.enqueue(task)
    const row = only(await jobRows(schema))
    expect(row.key_mode).toBe("debounce")
    expect(row.run_at.getTime() - row.created_at.getTime()).toBe(2000)
  })

  test("rejects invalid options before it calls the executor", async () => {
    let calls = 0
    const jobs = createJobs(
      () => {
        calls++
        return pool
      },
      { schemaName: schema.name },
    )
    await expect(jobs.enqueue(sendEmail, { to: "a" }, { maxAttempts: 0 })).rejects.toBeInstanceOf(
      RangeError,
    )
    expect(calls).toBe(0)
  })

  test("rejects a task name, a key, or a queue with a lone surrogate, also from withOptions, before it calls the executor", async () => {
    let calls = 0
    const jobs = createJobs(
      () => {
        calls++
        return pool
      },
      { schemaName: schema.name },
    )
    const staticQueue = defineTask("t", { queue: "q\udc00" })
    const computedKey = defineTask("t")
      .withPayload<string>()
      .withOptions(id => ({ key: `k:${id}` }))
    // A task defined by an older version of kern-jobs, whose defineTask accepted such a name.
    const olderTask = { ...defineTask("t"), name: "t\ud800" }
    await expect(jobs.enqueue(olderTask)).rejects.toThrow("The name of a task must be")
    await expect(jobs.enqueue(staticQueue)).rejects.toThrow("queue must be")
    // Cut inside an emoji.
    await expect(jobs.enqueue(computedKey, "😀".slice(0, 1))).rejects.toThrow("key must be")
    expect(calls).toBe(0)
  })
})
