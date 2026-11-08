// Entry of the driver of node-postgres, `kern-jobs/db/node-postgres`: the driver of kern-db that the worker and `migrateJobs` take over a node-postgres pool, from the copy of kern-db that kern-jobs runs on.
export { nodePostgresDriver } from "kern-db/node-postgres"
