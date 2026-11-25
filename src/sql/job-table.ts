// Job table of the SQL core, named in a PostgreSQL schema of its own.

import { isStorableText, storableTextRule, utf8Length } from "../limits"

import { quoteIdentifier } from "./postgres"

/**
 * Job table of the SQL core, in a PostgreSQL schema of its own.
 * Statements name it qualified by the schema, so that they do not depend on the `search_path` of a connection.
 */
export interface JobTable {
  /** Name of the PostgreSQL schema. */
  readonly schemaName: string
  /** Qualified name of the table in SQL. */
  readonly name: string
}

/** Name of the PostgreSQL schema of the job table when the application gives none. */
export const DEFAULT_SCHEMA_NAME = "kern_jobs"

/** Longest name of a PostgreSQL schema in bytes of UTF-8, `max_identifier_length` of a standard build of PostgreSQL. */
const maxSchemaNameBytes = 63

/**
 * Throws `TypeError` unless `schemaName` is storable text of 1 to 63 bytes in UTF-8.
 * PostgreSQL truncates a longer name, also a quoted one, so two names that differ only after its 63rd byte would name the same schema.
 */
export function checkSchemaName(schemaName: unknown): asserts schemaName is string {
  if (
    typeof schemaName !== "string" ||
    schemaName === "" ||
    !isStorableText(schemaName) ||
    utf8Length(schemaName) > maxSchemaNameBytes
  ) {
    throw new TypeError(
      `schemaName must be a string of 1 to ${maxSchemaNameBytes} bytes in UTF-8 that has ${storableTextRule}`,
    )
  }
}

/**
 * Names the job table of the SQL core in a PostgreSQL schema, and throws `TypeError` for a name that `checkSchemaName` rejects.
 */
export function createJobTable(schemaName: string): JobTable {
  checkSchemaName(schemaName)
  return { schemaName, name: `${quoteIdentifier(schemaName)}.job` }
}
