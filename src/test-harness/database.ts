// Test helpers of the SQL core: an isolated PostgreSQL schema per test, which the migrations of kern-jobs create, independent connections over postgres.js, breakpoints inside statements, and virtual time.
// Test connections do not add the test schema to `search_path`, so statements must qualify their names.
import { migrate } from "kern-migrations"
import { columnsOf, type Selectable } from "kern-orm"
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
  type SqlQueryTemplate,
  type SqlStatement,
  type StatementExecutor,
} from "kern-orm/db"
import { postgresJsDriver } from "kern-orm/db/postgres-js"
import { getBackendPid } from "kern-orm/testing"
import postgres from "postgres"

import { jobMigrations } from "../migration/migrations"
import { createJobTable, type JobTable } from "../sql/job-table"
import { quoteIdentifier } from "../sql/postgres"
import { bindWorkerDb, type WorkerDb } from "../worker/db/db"
import { jobTableSql } from "../worker/db/sql"
import { job } from "../worker/db/tables/job"

/** Returns the URL of the test database. */
export function databaseUrl(): string {
  const url = process.env.DATABASE_URL
  if (!url)
    throw new Error(
      "Set DATABASE_URL to a PostgreSQL database for tests, such as in .env.test.local.",
    )
  return url
}

// Blocks while the test holds the advisory lock of the breakpoint, see `holdHook`.
const createHookSql = (schemaName: string) => `
create function ${quoteIdentifier(schemaName)}.kern_test_hook(name text) returns boolean language plpgsql volatile as $$
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
  /** Breakpoints that block statements of this driver while the test holds them. */
  hooks?: string[]
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

/** Creates a postgres.js driver of a test database whose breakpoints call the hook function of its schema. */
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
  return options.hooks?.length ? withHooks(driver, schemaName, options.hooks) : driver
}

/**
 * Makes the given breakpoints of the SQL core, marked as `hook(name)` writes them, block the statements of a driver while the test holds them, by the hook function of a test schema.
 */
export function withHooks(driver: Driver, schemaName: string, hooks: string[]): Driver {
  const rewrite = (text: string) =>
    text.replace(/\/\*hook:([a-z_]+)\*\/true/g, (marker, name: string) =>
      hooks.includes(name)
        ? `(select ${quoteIdentifier(schemaName)}.kern_test_hook('${name}'))`
        : marker,
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
  /** Template of a statement on the connection, such as ``conn.query`begin` ``. */
  query: SqlQueryTemplate
  close(): Promise<void>
}

/** Opens a connection of a test schema on a client of its own. */
export async function connectTo(
  url: string,
  schemaName: string,
  options: TestDriverOptions = {},
): Promise<Conn> {
  const driver = createTestDriver(url, schemaName, { ...options, max: 1 })
  const connection = await driver.acquire()
  return {
    pid: await getBackendPid(connection),
    query: queryOn(connection),
    execute: statement => connection.execute(statement),
    executeScript: text => connection.executeScript(text),
    async close() {
      await connection.release()
      await driver.close()
    },
  }
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
  connect(options?: TestDriverOptions): Promise<Conn>
  /** Connection for setup, checks, and observation. */
  admin: Conn
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
  const conns: Conn[] = []
  const connect = async (options: TestDriverOptions = {}) => {
    const c = await connectTo(url, schemaName, options)
    conns.push(c)
    return c
  }
  const drivers = [createTestDriver(url, schemaName)]
  function createDriver(options?: TestDriverOptions): Driver {
    const driver = createTestDriver(url, schemaName, options)
    drivers.push(driver)
    return driver
  }
  const database = createDatabase({ driver: drivers[0]! })
  await migrate(database, jobMigrations, { schemaName })
  const admin = await connect()
  await admin.executeScript(createHookSql(schemaName))
  return {
    name: schemaName,
    table: createJobTable(schemaName),
    driver: drivers[0]!,
    database,
    db: bindWorkerDb(database, schemaName),
    createDriver,
    createDb(source) {
      const driver = isDriver(source) ? source : createDriver(source)
      return bindWorkerDb(createDatabase({ driver }), schemaName)
    },
    connect,
    admin,
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

/** Holds breakpoint `name`: statements of connections with this hook block in it until `release()`. */
export async function holdHook(
  conn: StatementExecutor,
  name: string,
): Promise<() => Promise<void>> {
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

/** Converts virtual time back to seconds. */
export const secondsOf = (d: Date) => (d.getTime() - BASE) / 1000

/** The least integer that a JavaScript number cannot hold, 2^53 + 1, which a number rounds to 2^53; a bigint id from it checks that the id is not read as a number. */
export const unsafeInteger = 2n ** 53n + 1n

/** Row of the job table with all its columns. */
export type JobRow = Selectable<typeof job>

/** Returns the job rows of a test schema that match the condition `where` of the alias `j`, in the order of their ids. */
export async function jobs(
  schema: Pick<TestSchema, "admin" | "table">,
  where: SqlFragment = sql`true`,
): Promise<JobRow[]> {
  const columns = columnsOf(job, "*", { alias: "j" })
  const rows = await schema.admin.query`
    select ${columns} from ${jobTableSql(schema.table)} j where ${where} order by j.id`.select({
    job: columns,
  })
  return rows.map(row => row.job)
}
