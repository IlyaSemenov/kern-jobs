// Statements of the worker whose rows it reads by the names of their columns, which a driver of kern-db gives with the result.
import type { Connection } from "kern-orm/db"
import { startSqlQuery } from "kern-orm/db/builder"

import { type Param, type TextRow, toStatement } from "../../sql/statement"

/** Target of the statements of the worker: an object whose `execute` is that of a `Connection` of kern-db, such as a driver, a connection, or a `Database`. */
export type StatementTarget = Pick<Connection, "execute">

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
    columns.forEach(({ name }, i) => (row[name] = values[i] ?? null))
    return row
  })
}

/** Starts a query of a statement with parameters that `encodeParam` turns into text, whose `select` decodes the fields of its rows by types; it runs once awaited. */
export function startQuery(target: StatementTarget, text: string, params: readonly Param[] = []) {
  return startSqlQuery(() => toStatement(text, params), target)
}
