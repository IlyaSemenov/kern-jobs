// SQL shared by the statements of the core: the time parameter, test breakpoints, quoted names, the range of counts that they take as `int`, the length of text in the bytes that PostgreSQL limits, and storable text, which PostgreSQL stores as given.

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

/** Returns the length of a string in bytes of UTF-8, the unit of the limits of names and columns of PostgreSQL. */
export function utf8Length(text: string): number {
  return new TextEncoder().encode(text).length
}

/**
 * Whether a string is storable text: it has no U+0000, which PostgreSQL rejects in text, and no lone surrogate.
 * Encoding as UTF-8 replaces a lone surrogate with U+FFFD, so two different strings, such as two keys, would become one value.
 */
export function isStorableText(text: string): boolean {
  return text.isWellFormed() && !text.includes("\0")
}

/** Requirement of storable text, for error messages. */
export const storableTextRule = "neither U+0000 nor a lone surrogate"

/** Throws `TypeError` for a value that is not storable text, naming it by `name`, such as `key`. */
export function checkStorableText(name: string, value: unknown): asserts value is string {
  if (typeof value !== "string" || !isStorableText(value)) {
    const got = typeof value === "string" ? JSON.stringify(value) : String(value)
    throw new TypeError(`${name} must be a string that has ${storableTextRule}, got ${got}`)
  }
}

/** Largest value of a PostgreSQL `int`. */
const maxInt = 2 ** 31 - 1

/**
 * Checks an option that counts, such as jobs or attempts, and that statements take as a PostgreSQL `int`: throws `RangeError` unless it is an integer from 1 to 2^31 − 1.
 */
export function checkCount(name: string, value: unknown): asserts value is number {
  if (!Number.isInteger(value) || (value as number) < 1 || (value as number) > maxInt) {
    throw new RangeError(`${name} must be an integer from 1 to ${maxInt}, got ${String(value)}`)
  }
}
