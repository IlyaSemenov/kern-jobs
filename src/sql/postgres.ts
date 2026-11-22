// SQL shared by the statements of the core: the time parameter, test breakpoints, and quoted names.

/**
 * SQL of the current time of a statement: the `$1` parameter when given, otherwise `statement_timestamp()`.
 * Every time-dependent statement of the core takes its time as `$1`, so tests can run on virtual time.
 */
export const NOW = `(select coalesce($1::text::timestamptz, statement_timestamp()))`

/**
 * Marks a breakpoint for deterministic tests: the inert expression `true`, which the test helpers replace on their connections with a call that blocks while the test holds the breakpoint.
 * The breakpoint runs after the statement has taken its snapshot.
 */
export const hook = (name: string) => `/*hook:${name}*/true`

/** Quotes a name for SQL, such as the name of a PostgreSQL schema. */
export function quoteIdentifier(name: string): string {
  return `"${name.replace(/"/g, '""')}"`
}
