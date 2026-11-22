// Unique index violations that cause a worker database operation to retry.
import { isQueryError } from "kern-orm/db"

/** Tells whether an error is a unique violation of the index `constraint`. */
export function isUniqueViolation(e: unknown, constraint: string): boolean {
  return isQueryError(e, "unique_violation") && e.constraint === constraint
}
