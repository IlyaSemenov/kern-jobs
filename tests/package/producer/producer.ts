// Enqueues a job of a project that only enqueues, through an executor that answers without a database, and prints the result as JSON; the project has neither the dependencies of kern-jobs nor a client library, so importing either fails.
import { createJobs, defineTask } from "kern-jobs"

const purgeExpiredSessionsTask = defineTask("sessions.purgeExpired")
// One row of one text column: the JSON array of the id and whether the enqueue inserted the row.
const jobs = createJobs(() => ({ query: async () => ({ rows: [['["1",true]']] }) }))
console.log(JSON.stringify(await jobs.enqueue(purgeExpiredSessionsTask)))
