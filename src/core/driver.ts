// Check of the driver of kern-db that an application passes.
import { type Driver, isDriver } from "kern-db"

/** Throws `TypeError` for a value that is not a driver of kern-db, such as a database client passed instead of its driver. */
export function checkDriver(driver: unknown): asserts driver is Driver {
  if (!isDriver(driver)) {
    throw new TypeError(
      "driver must be a driver of kern-db, such as postgresJsDriver(sql) of kern-jobs/db/postgres-js",
    )
  }
}
