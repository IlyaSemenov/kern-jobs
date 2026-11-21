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
    columns.forEach(({ name }, i) => (row[name] = values[i] ?? null))
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
