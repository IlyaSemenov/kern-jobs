// Test helpers of the SQL core: an isolated PostgreSQL schema per test, which the migrations of kern-jobs create, independent connections over postgres.js, pause points inside statements, and virtual time.
// Test connections do not add the test schema to `search_path`, so statements must qualify their names.

import { migrate } from "kern-migrations"
import { columnsOf, type DbSqlQueryTemplate, type Selectable } from "kern-orm"
import {
  type Connection,
  createDatabase,
  createStatement,
  type Database,
  type Driver,
  identifier,
  isDriver,
  queryOn,
  sql,
  type SqlFragment,
  type SqlStatement,
  type StatementExecutor,
} from "kern-orm/db"
import { postgresJsDriver } from "kern-orm/db/postgres-js"
import { getBackendPid } from "kern-orm/testing"
import postgres from "postgres"

import { jobMigrations } from "../migration/migrations"
import { createJobTable, type JobTable } from "../sql/job-table"
import { quoteIdentifier, replacePauseConditions } from "../sql/postgres"
import type { ValuesTarget } from "../sql/statement"
import { bindWorkerDb, schema, type WorkerDb } from "../worker/db/db"
import { job } from "../worker/db/tables/job"
import * as t from "../worker/db/types"

/** Returns the URL of the test database. */
export function databaseUrl(): string {
  const url = process.env.DATABASE_URL
  if (!url)
    throw new Error(
      "Set DATABASE_URL to a PostgreSQL database for tests, such as in .env.test.local.",
    )
  return url
}

// Blocks while the test holds the advisory lock of the pause point, see `pauseAt`.
const createPauseSql = (schemaName: string) => `
create function ${quoteIdentifier(schemaName)}.kern_test_pause(name text) returns boolean language plpgsql volatile as $$
begin
  perform pg_advisory_lock(1, hashtext(name));
  perform pg_advisory_unlock(1, hashtext(name));
  return true;
end
$$;
`

/** Options of a driver or connection of a test schema. */
export interface TestDriverOptions {
  /** Size of the pool. */
  max?: number
  /** Pause points that block statements of this driver while the test holds them. */
  pauses?: string[]
  /** `client_connection_check_interval` of the connections in milliseconds. */
  connectionCheckMs?: number
  /** `application_name` of the connections, by which a test finds their backends. */
  applicationName?: string
  /** `default_transaction_isolation` of the connections. */
  defaultIsolation?: "read committed" | "repeatable read" | "serializable"
  /** `DateStyle` of the connections, such as `SQL, DMY`. */
  dateStyle?: string
  /** `TimeZone` of the connections, such as `Asia/Kathmandu`. */
  timeZone?: string
}

/** Creates a postgres.js driver of a test database whose pause points call the pause function of its schema. */
export function createTestDriver(
  url: string,
  schemaName: string,
  options: TestDriverOptions = {},
): Driver {
  const sql = postgres(url, {
    max: options.max ?? 10,
    onnotice: () => {},
    connection: {
      ...(options.applicationName ? { application_name: options.applicationName } : {}),
      ...(options.connectionCheckMs
        ? { client_connection_check_interval: String(options.connectionCheckMs) }
        : {}),
      ...(options.defaultIsolation
        ? { default_transaction_isolation: options.defaultIsolation }
        : {}),
      ...(options.dateStyle ? { DateStyle: options.dateStyle } : {}),
      ...(options.timeZone ? { TimeZone: options.timeZone } : {}),
    },
  })
  const driver = postgresJsDriver(sql)
  return options.pauses?.length ? withPauses(driver, schemaName, options.pauses) : driver
}

/**
 * Makes the given pause points of the SQL core, marked as `pauseCondition(name)` writes them, block the statements of a driver from `pauseAt` until its resume, by the pause function of a test schema.
 */
export function withPauses(driver: Driver, schemaName: string, pauses: string[]): Driver {
  const rewrite = (text: string) =>
    replacePauseConditions(text, (name, condition) =>
      pauses.includes(name)
        ? `(select ${quoteIdentifier(schemaName)}.kern_test_pause('${name}'))`
        : condition,
    )
  const rewriteStatement = ({ text, parameters }: SqlStatement) =>
    createStatement(rewrite(text), parameters)
  return {
    execute: statement => driver.execute(rewriteStatement(statement)),
    executeScript: text => driver.executeScript(rewrite(text)),
    async acquire() {
      const connection = await driver.acquire()
      return {
        execute: statement => connection.execute(rewriteStatement(statement)),
        executeScript: text => connection.executeScript(rewrite(text)),
        release: () => connection.release(),
      }
    },
    close: () => driver.close(),
  }
}

/** Connection of a test, held until `close`, with the pid of its backend. */
export interface Conn extends Pick<Connection, "execute" | "executeScript"> {
  pid: number
  /** Template of a statement on the connection, which names the tables of the worker ORM schema in the test schema, such as ``conn.query`select count(*) from ${schema.job}` ``. */
  query: DbSqlQueryTemplate
  close(): Promise<void>
}

/** PostgreSQL schema of a test with the job table, its pools, and a factory of connections to it. */
export interface TestSchema {
  /** Name of the PostgreSQL schema. */
  name: string
  /** Job table of the schema. */
  table: JobTable
  /** Pool of the schema, for operations that reserve connections, such as the heartbeat of a worker. */
  driver: Driver
  /** Database over `driver`, for operations that run in its transactions, such as schedule evaluation; closing it would close `driver`. */
  database: Database
  /** Worker ORM schema bound to `database`. */
  db: WorkerDb
  /** Creates another pool of the schema, which `dispose` ends. */
  createDriver(options?: TestDriverOptions): Driver
  /** Creates a worker database over the given driver or a new pool with the given options. */
  createDb(source: Driver | TestDriverOptions): WorkerDb
  /** Opens a connection of the schema on a client of its own, which `dispose` closes. */
  connect(options?: TestDriverOptions): Promise<Conn>
  /** Connection for setup, checks, and observation. */
  conn: Conn
  /**
   * Names the job table of another PostgreSQL schema, `<name>_<label>`, that does not exist yet, for a test that creates it, such as by `migrateJobs`; `dispose` drops that schema.
   */
  unmigratedTable(label: string): JobTable
  /** Drops the schema and the schemas of `unmigratedTable`, and ends the pools and the connections. */
  dispose(): Promise<void>
}

/** Creates a PostgreSQL schema of a name of its own with the job table by the migrations of kern-jobs. */
export async function createTestSchema(url = databaseUrl()): Promise<TestSchema> {
  const schemaName = `t_${Math.random().toString(36).slice(2, 10)}`
  const schemaNames = [schemaName]
  const drivers = [createTestDriver(url, schemaName)]
  function createDriver(options?: TestDriverOptions): Driver {
    const driver = createTestDriver(url, schemaName, options)
    drivers.push(driver)
    return driver
  }
  const database = createDatabase({ driver: drivers[0]! })
  const db = bindWorkerDb(database, schemaName)
  const conns: Conn[] = []
  const connect = async (options: TestDriverOptions = {}): Promise<Conn> => {
    const driver = createTestDriver(url, schemaName, { ...options, max: 1 })
    const connection = await driver.acquire()
    const c: Conn = {
      pid: await getBackendPid(connection),
      query: db.$queryOn(connection),
      execute: statement => connection.execute(statement),
      executeScript: text => connection.executeScript(text),
      async close() {
        await connection.release()
        await driver.close()
      },
    }
    conns.push(c)
    return c
  }
  await migrate(database, jobMigrations, { schemaName })
  const conn = await connect()
  await conn.executeScript(createPauseSql(schemaName))
  return {
    name: schemaName,
    table: createJobTable(schemaName),
    driver: drivers[0]!,
    database,
    db,
    createDriver,
    createDb(source) {
      const driver = isDriver(source) ? source : createDriver(source)
      return bindWorkerDb(createDatabase({ driver }), schemaName)
    },
    connect,
    conn,
    unmigratedTable(label) {
      const table = createJobTable(`${schemaName}_${label}`)
      schemaNames.push(table.schemaName)
      return table
    },
    async dispose() {
      // The pool drops the schemas, since it reconnects when the connections of the test died, such as in a restart.
      for (const c of conns) await c.close().catch(() => {})
      for (const name of schemaNames) {
        await queryOn(drivers[0]!)`drop schema if exists ${identifier(name)} cascade`
      }
      for (const driver of drivers) await driver.close()
    },
  }
}

/** Pauses statements at pause point `name` on connections that enable it in `pauses`, and returns the function that resumes them. */
export async function pauseAt(conn: StatementExecutor, name: string): Promise<() => Promise<void>> {
  await queryOn(conn)`select pg_advisory_lock(1, hashtext(${name}))`
  return async () => {
    await queryOn(conn)`select pg_advisory_unlock(1, hashtext(${name}))`
  }
}

/** Returns the only element of a list, such as the claim of a statement with limit 1, and throws otherwise. */
export function only<T>(xs: T[]): T {
  if (xs.length !== 1) throw new Error(`expected one element, got ${xs.length}`)
  return xs[0]!
}

/** Returns the label of kern-jobs that starts the text of a statement, or `null` for a statement without one, such as `BEGIN`. */
export function getStatementLabel(text: string): string | null {
  return /^\/\* (kern-jobs:\S+) \*\//.exec(text)?.[1] ?? null
}

/** Wraps a target of statements so that it records the label of each statement that it executes, as `getStatementLabel` gives it, in `labels`. */
export function recordLabels(target: StatementExecutor): {
  target: StatementExecutor
  labels: (string | null)[]
} {
  const labels: (string | null)[] = []
  return {
    labels,
    target: {
      execute: statement => {
        labels.push(getStatementLabel(statement.text))
        return target.execute(statement)
      },
    },
  }
}

/** Checks that a promise has not settled within `ms`. */
export async function isPending(p: Promise<unknown>, ms = 50): Promise<boolean> {
  const marker = Symbol()
  const r = await Promise.race([
    p.then(
      () => null,
      () => null,
    ),
    sleep(ms).then(() => marker),
  ])
  return r === marker
}

/** Resolves after `ms` milliseconds. */
export const sleep = (ms: number) => new Promise<void>(r => setTimeout(r, ms))

const BASE = Date.UTC(2030, 0, 1)

/** Virtual time: seconds from a fixed point. */
export const T = (seconds: number) => new Date(BASE + Math.round(seconds * 1000))

/** Converts a time that the job table reads, in milliseconds since the epoch, back to virtual time in seconds. */
export const secondsOf = (milliseconds: number) => (milliseconds - BASE) / 1000

/** The least integer that a JavaScript number cannot hold, 2^53 + 1, which a number rounds to 2^53; a bigint id from it checks that the id is not read as a number. */
export const unsafeInteger = 2n ** 53n + 1n

/** Row of the job table with all its columns. */
export type JobRow = Selectable<typeof job>

/** Returns the job rows that match the condition `where` of the alias `j`, in the order of their ids, by a statement of `query`, a template of a `Db` of the worker ORM schema, such as `testSchema.conn.query`. */
export async function jobs(
  query: DbSqlQueryTemplate,
  where: SqlFragment = sql`true`,
): Promise<JobRow[]> {
  const columns = columnsOf(job, "*", { alias: "j" })
  const rows = await query`
    select ${columns} from ${schema.job} j where ${where} order by j.id`.select({
    job: columns,
  })
  return rows.map(row => row.job)
}

/** Returns the ids of the job rows of a scope in their order, by a statement of `query`, such as `testSchema.conn.query`. */
export async function idsOf(query: DbSqlQueryTemplate, scope: string): Promise<string[]> {
  return await query`
    select j.id from ${schema.job} j where j.scope = ${scope} order by j.id`.pluck(t.bigintText)
}

/** Returns a target of the statements of the core on a connection whose time, the `$1` parameter, is the given text, such as the time of a statement of a SQL function of the schema to the microsecond. */
export function atTime(conn: Pick<Conn, "execute">, now: string): ValuesTarget {
  return {
    execute: statement =>
      conn.execute({ ...statement, parameters: [now, ...statement.parameters.slice(1)] }),
  }
}
