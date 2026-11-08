// Entry of the driver of postgres.js, `kern-jobs/db/postgres-js`: the driver of kern-db that the worker and `migrateJobs` take over a postgres.js client, from the copy of kern-db that kern-jobs runs on.
export { postgresJsDriver } from "kern-db/postgres-js"
