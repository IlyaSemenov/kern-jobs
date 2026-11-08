// Statements of the core: parameters encoded as text, and rows read by the names of their columns on a driver, or by the position of their values on a driver or the executor of an application.
import type { Connection, SqlStatement, TextValue } from "kern-db"

/** Target of the statements of the core: a driver, which runs them on any connection of its pool, or a connection. */
export type StatementTarget = Pick<Connection, "execute">

/**
 * Target of a statement whose rows the core reads by the position of their values: a statement target, or the executor of an application through `fromExecutor`, whose client may rename the columns.
 */
export interface ValuesTarget {
  execute(statement: SqlStatement): Promise<{ readonly rows: readonly (readonly TextValue[])[] }>
}

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
  // `sql` of postgres.js is a function too, so its `unsafe` tells it from a function that returns a client.
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
  if (typeof client === "function" && !hasMethod(client, "unsafe")) {
    const getClient = client as () => JobsConnection
    return () => toClientTarget(getClient())
  }
  const target = toClientTarget(client)
  return () => target
}

/** Value of a statement parameter, which `encodeParam` turns into text. */
export type Param = string | number | boolean | Date | readonly string[] | null | undefined

/** Row of a result: the text of each column by its name, or null. */
export type TextRow = Record<string, string | null>

/** Converts a parameter into the text that PostgreSQL reads by the type that the SQL casts it to. */
export function encodeParam(value: Param): string | null {
  if (value == null) return null
  if (typeof value === "string") return value
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new TypeError(`Cannot encode the parameter ${value}`)
    return String(value)
  }
  if (typeof value === "boolean") return value ? "true" : "false"
  if (value instanceof Date) return value.toISOString()
  return `{${value.map(item => `"${item.replace(/[\\"]/g, "\\$&")}"`).join(",")}}`
}

/**
 * Builds a statement with parameters that `encodeParam` turns into text.
 *
 * It builds the object itself rather than with `createStatement` of kern-db, so that the enqueue of the main entry does not load kern-db.
 */
function toStatement(text: string, params: readonly Param[]): SqlStatement {
  return { text, parameters: params.map(encodeParam) }
}

/**
 * Executes a statement with parameters that `encodeParam` turns into text, and returns its rows by the names of their columns; of several columns of one name, the last one wins.
 */
export async function query(
  target: StatementTarget,
  text: string,
  params: readonly Param[] = [],
): Promise<TextRow[]> {
  const { columns, rows } = await target.execute(toStatement(text, params))
  return rows.map(values => {
    const row: TextRow = {}
    columns.forEach((column, i) => (row[column] = values[i] ?? null))
    return row
  })
}

/** Executes a statement with parameters that `encodeParam` turns into text, and returns its rows as arrays of values. */
export async function queryValues(
  target: ValuesTarget,
  text: string,
  params: readonly Param[] = [],
): Promise<readonly (readonly TextValue[])[]> {
  return (await target.execute(toStatement(text, params))).rows
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
