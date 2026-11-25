// SQL shared by the statements of the core: the time parameter, test pause points, and quoted names.

/**
 * SQL of the current time of a statement of SQL text: the `$1` parameter when given, otherwise `statement_timestamp()`.
 * Every time-dependent statement of SQL text takes its time as `$1`, so tests can run on virtual time.
 */
export const NOW = `(select coalesce($1::text::timestamptz, statement_timestamp()))`

/**
 * Condition of a pause point of a statement for deterministic tests: the inert expression `true`, which the test helpers replace on their connections with a call that blocks from the pause of the test until its resume and then returns `true`.
 * A statement pauses there after it has taken its snapshot.
 * A template interpolates `pauseSql(name)` of `worker/db/sql.ts` instead, since it would pass a bare string as a parameter.
 */
export const pauseCondition = (name: string) => `/*pause:${name}*/true`

/** Replaces each condition of `pauseCondition` in SQL text by what `replace` returns for its name and its text. */
export function replacePauseConditions(
  text: string,
  replace: (name: string, condition: string) => string,
): string {
  return text.replace(/\/\*pause:([a-z_]+)\*\/true/g, (condition, name: string) =>
    replace(name, condition),
  )
}

/** Quotes a name for SQL, such as the name of a PostgreSQL schema. */
export function quoteIdentifier(name: string): string {
  return `"${name.replace(/"/g, '""')}"`
}
