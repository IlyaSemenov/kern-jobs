// SQL shared by the statements of the core: the time parameter, test breakpoints, and quoted names.

/**
 * SQL of the current time of a statement of SQL text: the `$1` parameter when given, otherwise `statement_timestamp()`.
 * Every time-dependent statement of SQL text takes its time as `$1`, so tests can run on virtual time.
 */
export const NOW = `(select coalesce($1::text::timestamptz, statement_timestamp()))`

/**
 * Marks a breakpoint in SQL text for deterministic tests: the inert expression `true`, which the test helpers replace on their connections with a call that blocks while the test holds the breakpoint.
 * The breakpoint runs after the statement has taken its snapshot.
 * A template interpolates it as `unsafeSql(hook(name))`, since it would pass a bare string as a parameter.
 */
export const hook = (name: string) => `/*hook:${name}*/true`

/** Quotes a name for SQL, such as the name of a PostgreSQL schema. */
export function quoteIdentifier(name: string): string {
  return `"${name.replace(/"/g, '""')}"`
}
