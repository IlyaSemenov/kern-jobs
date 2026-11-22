// Limits of the values that the API takes and PostgreSQL stores: storable text, the length of text in the bytes that PostgreSQL limits, and counts that statements take as `int`.

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
