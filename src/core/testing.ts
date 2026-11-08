// Test helpers of the SQL core: an isolated PostgreSQL schema per test, which the migrations of kern-jobs create, independent connections over postgres.js, breakpoints inside statements, virtual time, and seeded random inputs.
// Test connections do not add the test schema to `search_path`, so statements must qualify their names.
import {
  type Connection,
  createDatabase,
  createStatement,
  type Database,
  type Driver,
  type SqlStatement,
} from "kern-db"
import { postgresJsDriver } from "kern-db/postgres-js"
import { getBackendPid } from "kern-db/testing"
import { migrate } from "kern-migrations"
import postgres from "postgres"
import { uniformFloat64 } from "pure-rand/distribution/uniformFloat64"
import { uniformInt } from "pure-rand/distribution/uniformInt"
import { xoroshiro128plus } from "pure-rand/generator/xoroshiro128plus"

import { jobMigrations } from "../migration/migrations"

import { quoteIdentifier } from "./db"
import { jobColumns, type JobRow, toJob } from "./job"
import { createJobTable, type JobTable } from "./schema"
import { query, type StatementTarget } from "./statement"

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
    },
  })
  const driver = postgresJsDriver(sql)
  return options.hooks?.length ? withHooks(driver, schemaName, options.hooks) : driver
}

/**
 * Makes the given breakpoints of the SQL core, marked with `hook(name)`, block the statements of a driver while the test holds them, by the hook function of a test schema.
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
  /** Database over `driver`, for fail, which runs in its transaction; closing it would close `driver`. */
  database: Database
  /** Creates another pool of the schema, which `dispose` ends. */
  createDriver(options?: TestDriverOptions): Driver
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
  const database = createDatabase({ driver: drivers[0]! })
  await migrate(database, jobMigrations, { schemaName })
  const admin = await connect()
  await admin.executeScript(createHookSql(schemaName))
  return {
    name: schemaName,
    table: createJobTable(schemaName),
    driver: drivers[0]!,
    database,
    createDriver(options) {
      const driver = createTestDriver(url, schemaName, options)
      drivers.push(driver)
      return driver
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
        await query(drivers[0]!, `drop schema if exists ${quoteIdentifier(name)} cascade`)
      }
      for (const driver of drivers) await driver.close()
    },
  }
}

/** Holds breakpoint `name`: statements of connections with this hook block in it until `release()`. */
export async function holdHook(conn: StatementTarget, name: string): Promise<() => Promise<void>> {
  await query(conn, `select pg_advisory_lock(1, hashtext($1::text))`, [name])
  return async () => {
    await query(conn, `select pg_advisory_unlock(1, hashtext($1::text))`, [name])
  }
}

/** Returns the only element of a list, such as the claim of a statement with limit 1, and throws otherwise. */
export function only<T>(xs: T[]): T {
  if (xs.length !== 1) throw new Error(`expected one element, got ${xs.length}`)
  return xs[0]!
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

/**
 * Creates a source of random test inputs that repeats its sequence for the same seed.
 *
 * `float()` returns a number from 0 inclusive to 1 exclusive, and `int(min, max)` an integer from `min` to `max` inclusive.
 */
export function createRandom(seed: number) {
  const rng = xoroshiro128plus(seed)
  return {
    float: () => uniformFloat64(rng),
    int: (min: number, max: number) => uniformInt(rng, min, max),
  }
}

const BASE = Date.UTC(2030, 0, 1)

/** Virtual time: seconds from a fixed point. */
export const T = (seconds: number) => new Date(BASE + Math.round(seconds * 1000))

/** Converts virtual time back to seconds. */
export const secondsOf = (d: Date) => (d.getTime() - BASE) / 1000

/** Returns the job rows of a test schema in the order of their ids. */
export async function jobs(
  schema: Pick<TestSchema, "admin" | "table">,
  where = "true",
): Promise<JobRow[]> {
  const rows = await query(
    schema.admin,
    `select ${jobColumns("j")} from ${schema.table.name} j where ${where} order by id`,
  )
  return rows.map(toJob)
}
