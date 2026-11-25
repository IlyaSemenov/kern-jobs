import { describe, test } from "bun:test"

import { defineTask, type FailureImplementation, type TaskImplementation } from "../task/task"

import { runFailure, type RunFailureJob, type RunFailureOptions } from "./run-failure"

const sendEmailTask = defineTask("email.send").withPayload<{ to: string }>()
const purgeExpiredSessionsTask = defineTask("sessions.purgeExpired")
const reportTask = defineTask("reports.build", { scoped: true }).withPayload<{ id: number }>()

describe("runFailure", () => {
  test("takes an implementation with onFailure and the payload of its task, optional for a void payload", () => {
    const sendEmail = sendEmailTask.implement(() => {}, { onFailure: () => {} })
    void runFailure(sendEmail, { to: "a" }, { reason: "lease-expired", job: { attempt: 2 } })
    void runFailure(purgeExpiredSessionsTask.implement(() => {}, { onFailure: () => {} }))
    const withoutFailure = sendEmailTask.implement(() => {})
    // @ts-expect-error The implementation needs onFailure.
    void runFailure(withoutFailure, { to: "a" })
    // @ts-expect-error The task needs a payload.
    void runFailure(sendEmail)
    // @ts-expect-error The payload has the type of the task.
    void runFailure(sendEmail, { to: 1 })
    // @ts-expect-error The reason is one of a failure.
    void runFailure(sendEmail, { to: "a" }, { reason: "crashed" })
  })

  test("takes the scope of the failed job of a scoped task", () => {
    const implementation = reportTask.implement(() => {}, { onFailure: () => {} })
    void runFailure(implementation, { id: 1 }, { job: { scope: "a" } })
    // @ts-expect-error A scoped task needs the scope of its job.
    void runFailure(implementation, { id: 1 })
    // @ts-expect-error A scoped task needs the scope of its job.
    void runFailure(implementation, { id: 1 }, { job: { attempt: 2 } })
    // @ts-expect-error The scope of a scoped job is a string.
    void runFailure(implementation, { id: 1 }, { job: { scope: null } })
    // @ts-expect-error The options of a scoped task need the scope of its job.
    const missing: RunFailureOptions<string> = {}
    void missing
  })

  test("takes the arguments of a wrapper generic over the payload without a cast", () => {
    const runWithScope = <P>(
      implementation: TaskImplementation<string, P, any> & {
        readonly failure: FailureImplementation<string, P, any>
      },
      payload: P,
      job: RunFailureJob = {},
    ) => runFailure(implementation, payload, { job: { scope: "a", ...job } })
    void runWithScope(
      reportTask.implement(() => {}, { onFailure: () => {} }),
      { id: 1 },
    )
    void runWithScope(
      sendEmailTask.implement(() => {}, { onFailure: () => {} }),
      { to: "a" },
      { attempt: 2 },
    )
  })
})
