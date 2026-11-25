// SQL fragment of the time of a statement, shared by the hand-written statements of the worker.

import { param, sql, type SqlFragment } from "kern-orm/db"

import * as t from "./types"

/**
 * Returns SQL of the current time of a statement: `now` when given, so that tests can run on virtual time, otherwise `statement_timestamp()`.
 * A statement binds one parameter wherever it interpolates the same fragment, so create it once per statement.
 */
export function nowSql(now: Date | null | undefined): SqlFragment {
  return sql`(select coalesce(${param(t.timestamptz.nullable(), now ?? null)}, statement_timestamp()))`
}
