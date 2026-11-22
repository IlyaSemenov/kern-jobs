// Entry of the driver of postgres.js, `kern-jobs/drivers/postgres-js`: the driver of kern-db that the worker and `migrateJobs` take over a postgres.js client, from the copy of kern-db that kern-jobs runs on.
export { postgresJsDriver } from "kern-orm/db/postgres-js"
