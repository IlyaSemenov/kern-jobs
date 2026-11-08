// Unique violations of the indexes that enforce the invariants of keys and queues, after which the operations of the core retry; apart from the SQL of the statements, so that the enqueue of the main entry does not load kern-db.
import { isQueryError } from "kern-db"

/** Tells whether an error is a unique violation of the index `constraint`. */
export function isUniqueViolation(e: unknown, constraint: string): boolean {
  return isQueryError(e, "unique_violation") && e.constraint === constraint
}
