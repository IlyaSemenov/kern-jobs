// Job table of the SQL core with its claim sequence, named in a PostgreSQL schema of their own.
import { quoteIdentifier, utf8Length } from "./db"

/**
 * Job table of the SQL core with its claim sequence, in a PostgreSQL schema of their own.
 * Statements name them qualified by the schema, so that they do not depend on the `search_path` of a connection.
 */
export interface JobTable {
  /** Name of the PostgreSQL schema. */
  readonly schemaName: string
  /** Qualified name of the table in SQL. */
  readonly name: string
  /** Qualified name of the claim sequence in SQL. */
  readonly sequence: string
}

/** Name of the PostgreSQL schema of the job table when the application gives none. */
export const DEFAULT_SCHEMA = "kern_jobs"

/** Longest name of a PostgreSQL schema in bytes of UTF-8, `max_identifier_length` of a standard build of PostgreSQL. */
const maxSchemaNameBytes = 63

/**
 * Throws `TypeError` unless `schemaName` is a string of 1 to 63 bytes in UTF-8 without U+0000.
 * PostgreSQL truncates a longer name, also a quoted one, so two names that differ only after its 63rd byte would name the same schema; it rejects U+0000 in the text of a statement.
 */
export function checkSchemaName(schemaName: unknown): asserts schemaName is string {
  if (
    typeof schemaName !== "string" ||
    schemaName === "" ||
    schemaName.includes("\0") ||
    utf8Length(schemaName) > maxSchemaNameBytes
  ) {
    throw new TypeError(
      `schemaName must be a string of 1 to ${maxSchemaNameBytes} bytes in UTF-8 without U+0000`,
    )
  }
}

/**
 * Names the job table of the SQL core in a PostgreSQL schema, and throws `TypeError` for a name that `checkSchemaName` rejects.
 */
export function createJobTable(schemaName: string): JobTable {
  checkSchemaName(schemaName)
  const s = quoteIdentifier(schemaName)
  return { schemaName, name: `${s}.job`, sequence: `${s}.claim_seq` }
}
