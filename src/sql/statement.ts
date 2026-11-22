// Statements of the core: parameters encoded as text, and rows read by the position of their values on a driver, a database, or the executor of an application.
// The main entry runs these statements, so this module must not import kern-db or kern-orm, not even their types.

/** Statement of the core: its text and the text of each parameter, which a driver or a database of kern-db takes as its `SqlStatement`. */
export interface Statement {
  readonly text: string
  readonly parameters: readonly (string | null)[]
}

/**
 * Target of a statement whose rows the core reads by the position of their values: a driver, a connection, or a database of kern-db, or the executor of an application through `fromExecutor`, whose client may rename the columns.
 */
export interface ValuesTarget {
  execute(statement: Statement): Promise<{ readonly rows: readonly (readonly (string | null)[])[] }>
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

/** Builds a statement with parameters that `encodeParam` turns into text. */
export function toStatement(text: string, params: readonly Param[]): Statement {
  return { text, parameters: params.map(encodeParam) }
}

/** Executes a statement with parameters that `encodeParam` turns into text, and returns its rows as arrays of values. */
export async function queryValues(
  target: ValuesTarget,
  text: string,
  params: readonly Param[] = [],
): Promise<readonly (readonly (string | null)[])[]> {
  return (await target.execute(toStatement(text, params))).rows
}
