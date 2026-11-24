// SQL fragments shared by the hand-written statements of the worker: the time of a statement and the name of the job table.
import { identifier, param, sql, type SqlFragment } from "kern-orm/db"

import type { JobTable } from "../../sql/job-table"

import * as t from "./types"

/**
 * Returns SQL of the current time of a statement: `now` when given, so that tests can run on virtual time, otherwise `statement_timestamp()`.
 * A statement binds one parameter wherever it interpolates the same fragment, so create it once per statement.
 */
export function nowSql(now: Date | null | undefined): SqlFragment {
  return sql`(select coalesce(${param(t.timestamptz.nullable(), now ?? null)}, statement_timestamp()))`
}

/** Returns SQL of the name of the job table, qualified by its PostgreSQL schema. */
export function jobTableSql(table: JobTable): SqlFragment {
  return identifier(table.schemaName, "job")
}
