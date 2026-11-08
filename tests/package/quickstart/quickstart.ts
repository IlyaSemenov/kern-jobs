import { createJobs, defineTask } from "kern-jobs"
import { postgresJsDriver } from "kern-jobs/db/postgres-js"
import { migrateJobs } from "kern-jobs/migrations"
import { defineManifest, startWorker } from "kern-jobs/worker"
import postgres from "postgres"

const sql = postgres(process.env.DATABASE_URL!)
const driver = postgresJsDriver(sql)

// Create the schema of kern-jobs, or update it.
await migrateJobs(driver)

// Define a task and enqueue a job of it.
const greetTask = defineTask("greet").withPayload<{ name: string }>()

const jobs = createJobs(sql)
await jobs.enqueue(greetTask, { name: "world" })

// Run a worker until it has run the job, and stop it.
const greeted = Promise.withResolvers<void>()

const greet = greetTask.implement(async ({ name }) => {
  console.log(`Hello, ${name}!`)
  greeted.resolve()
})

const worker = startWorker(driver, { manifest: defineManifest({ tasks: [greet] }) })
await greeted.promise
await worker.stop()
await sql.end()
