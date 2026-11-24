// Key modes and payloads of jobs, and their PostgreSQL representation.

/** Key mode of an enqueue, stored in the row by its latest enqueue. */
export type KeyMode = "replace" | "keep" | "debounce"

/**
 * Returns the JSON text of a payload that the table stores, or `undefined` for a payload that JSON has no text for, such as `undefined` or an object whose `toJSON` returns `undefined`, which the table stores as SQL NULL.
 * It throws the `TypeError` of `JSON.stringify`, such as for a bigint or a circular object.
 */
export function encodePayload(payload: unknown): string | undefined {
  // `JSON.stringify` returns `undefined` for such a value, though its declared type is `string`.
  return JSON.stringify(payload) as string | undefined
}

/** Returns the payload that a handler gets from the JSON text of the table: `undefined` for SQL NULL. */
export function decodePayload(json: string | null | undefined): unknown {
  return json == null ? undefined : JSON.parse(json)
}
