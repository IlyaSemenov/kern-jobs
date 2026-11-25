// SQL fragments shared by the hand-written statements of the worker.

import { param, sql, type SqlFragment, unsafeSql } from "kern-orm/db"

import { pauseCondition } from "../../sql/postgres"

import * as t from "./types"

/**
 * Returns SQL of the current time of a statement: `now` when given, so that tests can run on virtual time, otherwise `statement_timestamp()`.
 * A statement binds one parameter wherever it interpolates the same fragment, so create it once per statement.
 */
export function nowSql(now: Date | null | undefined): SqlFragment {
  return sql`(select coalesce(${param(t.timestamptz.nullable(), now ?? null)}, statement_timestamp()))`
}

/** Returns the condition of pause point `name` of a statement for deterministic tests, the inert `true` of `pauseCondition`, as a fragment of a template. */
export function pauseSql(name: string): SqlFragment {
  return unsafeSql(pauseCondition(name))
}
