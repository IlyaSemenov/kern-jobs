// Adapts the database client of an application to the statements of its jobs API.

import type { ValuesTarget } from "../sql/statement"

/**
 * Database client of an application that runs the statements of its enqueues, such as a `pg.Pool`, a `pg.PoolClient`, or a wrapper of `sql.unsafe` of postgres.js.
 */
export interface Executor {
  /**
   * Executes a statement with parameters `$1`, `$2`, … and returns its rows, each as an object of values by the names of the columns or as an array of values.
   */
  query(
    text: string,
    values: (string | null)[],
  ): Promise<{ readonly rows: readonly (Readonly<Record<string, unknown>> | readonly unknown[])[] }>
}

/** Rows of a result of a client: each an object of values by the names of the columns or an array of values. */
type ClientRows = readonly (Readonly<Record<string, unknown>> | readonly unknown[])[]

/** Client of postgres.js, `sql` or a transaction of it, whose `unsafe` runs a statement with parameters. */
export interface UnsafeClient {
  unsafe(text: string, values: (string | null)[]): PromiseLike<ClientRows>
}

/** Client that an enqueue runs on: an executor, such as a `pg.Pool` or a `pg.PoolClient`, or a client of postgres.js. */
export type JobsConnection = Executor | UnsafeClient

/**
 * Database of kern-db or kern-orm, as `createJobs` takes it: its `execute` runs a statement in the current transaction of the database and gives the values of the rows as text.
 */
export interface JobsDatabase {
  execute(statement: {
    readonly text: string
    readonly parameters: readonly (string | null)[]
  }): Promise<{ readonly rows: readonly (readonly (string | null)[])[] }>
}

/** `Db` of kern-orm, as `createJobs` takes it: a schema bound to a database. */
export interface JobsDb {
  readonly $database: JobsDatabase
}

/**
 * Database client of `createJobs`: a client that every enqueue runs on, a function that returns the client of the next enqueue, such as of the current transaction, a database of kern-db, or a `Db` of kern-orm.
 */
export type JobsClient = JobsConnection | (() => JobsConnection) | JobsDatabase | JobsDb

function hasMethod(value: unknown, name: string): boolean {
  return (
    (typeof value === "object" || typeof value === "function") &&
    value !== null &&
    typeof (value as Record<string, unknown>)[name] === "function"
  )
}

/**
 * Returns the statement target of a client that the statements of `jobs` run on, a connection, a database, or a `Db`, and throws `TypeError` for another value.
 */
function toClientTarget(connection: unknown): ValuesTarget {
  const database = hasMethod((connection as Partial<JobsDb> | null)?.$database, "execute")
    ? (connection as JobsDb).$database
    : connection
  // A database of kern-db also has `query`, which takes a template, so `execute` tells it first.
  if (hasMethod(database, "execute")) return database as JobsDatabase
  if (hasMethod(connection, "unsafe")) {
    const client = connection as UnsafeClient
    return fromExecutor({
      query: async (text, values) => ({ rows: await client.unsafe(text, values) }),
    })
  }
  if (hasMethod(connection, "query")) return fromExecutor(connection as Executor)
  throw new TypeError(
    "client must be a pg.Pool, a client of postgres.js, an executor, a function that returns one of them, a Database of kern-db, or a Db of kern-orm",
  )
}

/**
 * Returns the function that gives the statement target of the next call of `jobs`, such as an enqueue, on a client of `createJobs`, and throws `TypeError` for a value that is none of its forms; a function that returns a client is checked at each call.
 */
export function toJobsTarget(client: JobsClient): () => ValuesTarget {
  // Both postgres.js and custom executors can be callable clients.
  if (typeof client === "function" && !hasMethod(client, "unsafe") && !hasMethod(client, "query")) {
    const getClient = client as () => JobsConnection
    return () => toClientTarget(getClient())
  }
  const target = toClientTarget(client)
  return () => target
}

/**
 * Runs the statements of the core on the executor of an application.
 * The client of the application may rename the columns of a result, such as `postgres.camel`, so its rows give only the values in the order of the columns, and the statements select only text, which clients do not parse.
 * `undefined` counts as `null`, and a value of another type means that the client transformed it, which throws `TypeError`.
 */
export function fromExecutor(executor: Executor): ValuesTarget {
  return {
    async execute({ text, parameters }) {
      const { rows } = await executor.query(text, [...parameters])
      return {
        rows: rows.map(row =>
          (Array.isArray(row) ? row : Object.values(row)).map((value: unknown) => {
            if (value == null) return null
            if (typeof value === "string") return value
            throw new TypeError(
              `The executor returned a ${typeof value} instead of text: its client must not transform text values`,
            )
          }),
        ),
      }
    },
  }
}
