import { describe, test } from "bun:test"

import { defineTask } from "../task/task"

import { runJob, type RunJobOptions } from "./run-job"

const sendEmailTask = defineTask("email.send").withPayload<{ to: string }>()
const purgeExpiredSessionsTask = defineTask("sessions.purgeExpired")
const reportTask = defineTask("reports.build", { scoped: true }).withPayload<{ id: number }>()

describe("runJob", () => {
  test("takes the payload of the task, optional for a void payload", () => {
    const sendEmail = sendEmailTask.implement(() => {})
    void runJob(sendEmail, { to: "a" })
    void runJob(purgeExpiredSessionsTask.implement(() => {}))
    // @ts-expect-error The task needs a payload.
    void runJob(sendEmail)
    // @ts-expect-error The payload has the type of the task.
    void runJob(sendEmail, { to: 1 })
  })

  test("takes the scope of the job of a scoped task", () => {
    const implementation = reportTask.implement(() => {})
    void runJob(implementation, { id: 1 }, { job: { scope: "a" } })
    // @ts-expect-error A scoped task needs the scope of its job.
    void runJob(implementation, { id: 1 })
    // @ts-expect-error A scoped task needs the scope of its job.
    void runJob(implementation, { id: 1 }, { job: { attempt: 2 } })
    // @ts-expect-error The scope of a scoped job is a string.
    void runJob(implementation, { id: 1 }, { job: { scope: null } })
  })

  test("takes options of the public type of a scoped task, which require the scope of its job", () => {
    const options: RunJobOptions<string> = { job: { scope: "a" } }
    void runJob(
      reportTask.implement(() => {}),
      { id: 1 },
      options,
    )
    // @ts-expect-error The options of a scoped task need the scope of its job.
    const missing: RunJobOptions<string> = {}
    void missing
  })

  test("takes a null scope or a string for a task that is not scoped", () => {
    void runJob(
      sendEmailTask.implement(() => {}),
      { to: "a" },
      { job: { scope: null } },
    )
    void runJob(
      sendEmailTask.implement(() => {}),
      { to: "a" },
      { job: { scope: "a", attempt: 2 } },
    )
  })
})
