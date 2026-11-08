// Enqueues, retries, and deletes through the clients of an application: node-postgres and postgres.js, with their transactions, renamed columns, and parsers of their own, and a Database of kern-db and a Db of kern-orm.
import { afterEach, beforeEach, describe, expect, test } from "bun:test"

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

import { createJobs, type JobsOptions } from "./jobs"

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

/** Marks a pending job of the test schema failed, as a worker does after the last attempt of the job. */
const markFailed = (id: string) =>
  query(
    schema.admin,
    `update ${schema.table.name} set state = 'failed', attempts = 1, last_error = 'boom', failed_at = now()
     where id = $1::text::bigint`,
    [id],
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

    test("a retry and a delete in a transaction that rolls back change nothing", async () => {
      const jobs = createTestJobs()
      const { id: retried } = await jobs.enqueue(sendEmail, { to: "a@example.com" })
      const { id: deleted } = await jobs.enqueue(sendEmail, { to: "b@example.com" })
      await markFailed(retried)
      const before = await jobRows(schema)
      const error = new Error("rollback")
      await expect(
        client.transaction(async () => {
          expect(await jobs.retry(retried)).toEqual({ retried: true })
          expect(await jobs.delete(deleted)).toBe(true)
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
    expect(() => createJobs(executor, { schemaName } as unknown as JobsOptions)).toThrow(
      new TypeError("schemaName must be a string of 1 to 63 bytes in UTF-8 without U+0000"),
    )
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
    for (const scope of ["a", tenant, null, undefined]) {
      await createJobs(pool, { schemaName: schema.name, scope }).enqueue(sendEmail, { to: "a" })
    }
    expect([await scopes(), calls]).toEqual([["", "", "", ""], 0])
  })

  test("rejects an enqueue of a scoped task without a scope or with an invalid one that its function returns, before it calls the client", async () => {
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
    // As a JavaScript function returns it, past the types.
    await expect(enqueueIn(() => undefined as unknown as string)).rejects.toThrow(
      new TypeError("scope must be a string, got undefined"),
    )
    await expect(enqueueIn(() => "")).rejects.toThrow(
      new RangeError('scope must be 1 to 128 bytes in UTF-8, got ""'),
    )
    expect(calls).toBe(0)
  })

  test("retries and deletes only the jobs of the scope of createJobs, a string or what its function returns at the call, and the jobs of every scope without one", async () => {
    const enqueueIn = (scope: string) =>
      createJobs(pool, { schemaName: schema.name, scope }).enqueue(indexProduct)
    const { id: failed } = await enqueueIn("a")
    const { id: pending } = await enqueueIn("b")
    await markFailed(failed)
    let tenant = "b"
    const jobs = createJobs(pool, { schemaName: schema.name, scope: () => tenant })
    expect([await jobs.retry(failed), await jobs.delete(failed)]).toEqual([null, false])
    tenant = "a"
    expect(await jobs.retry(failed)).toEqual({ retried: true })
    expect(await createJobs(pool, { schemaName: schema.name, scope: "a" }).delete(pending)).toBe(
      false,
    )
    const everyScope = createJobs(pool, { schemaName: schema.name })
    expect([await everyScope.delete(failed), await everyScope.delete(pending)]).toEqual([
      true,
      true,
    ])
  })

  test("rejects a retry and a delete whose scope function throws or returns an invalid scope, before it calls the client", async () => {
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
    await expect(jobsIn(() => "").delete("1")).rejects.toThrow(
      new RangeError('scope must be 1 to 128 bytes in UTF-8, got ""'),
    )
    expect(calls).toBe(0)
  })

  test("createJobs throws TypeError for a scope other than a string or a function, and RangeError for an empty one or one longer than 128 bytes", () => {
    const executor = { query: () => Promise.reject(new Error("a statement ran")) }
    const create = (scope: unknown) => () =>
      createJobs(executor, { scope } as unknown as JobsOptions)
    expect(create(42)).toThrow(
      new TypeError("scope must be a string or a function that returns one"),
    )
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
})
