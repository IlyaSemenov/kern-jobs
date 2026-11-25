import { describe, expectTypeOf, test } from "bun:test"

import type { Jobs } from "../producer/jobs"
import { defineTask, type TaskImplementation } from "../task/task"

import type { RunJobInfo } from "./run-job"
import { createTestJobs, type TestJobCall, type TestJobQueries } from "./test-jobs"

const sendEmailTask = defineTask("email.send").withPayload<{ to: string }>()
const purgeExpiredSessionsTask = defineTask("sessions.purgeExpired")
const reportTask = defineTask("reports.build", { scoped: true }).withPayload<{ id: number }>()

describe("createTestJobs", () => {
  test("is a Jobs, so that an application provides it where it takes the jobs API", () => {
    expectTypeOf(createTestJobs()).toExtend<Jobs>()
  })

  test("types the calls and the payloads of a task by its payload, and the result of capture by the action", async () => {
    const jobs = createTestJobs()
    const captured = await jobs.capture(async () => 42)
    expectTypeOf(captured.result).toEqualTypeOf<number>()
    expectTypeOf(captured.callsFor(sendEmailTask)).toEqualTypeOf<TestJobCall<{ to: string }>[]>()
    expectTypeOf(captured.payloadsFor(reportTask)).toEqualTypeOf<{ id: number }[]>()
    expectTypeOf(captured.payloadsFor(purgeExpiredSessionsTask)).toEqualTypeOf<void[]>()
  })

  test("types the result of capture as await gives it, of a value, a promise, or another thenable", async () => {
    const jobs = createTestJobs()
    expectTypeOf((await jobs.capture(() => 1)).result).toEqualTypeOf<number>()
    expectTypeOf((await jobs.capture(async () => "a")).result).toEqualTypeOf<string>()
    expectTypeOf(
      (await jobs.capture(() => ({}) as PromiseLike<boolean>)).result,
    ).toEqualTypeOf<boolean>()
  })

  test("runJob takes the payload of the task, optional for a void payload, and an optional scope for a scoped task", () => {
    const jobs = createTestJobs({ scope: "a" })
    const sendEmail = sendEmailTask.implement(() => {})
    void jobs.runJob(sendEmail, { to: "a" }, { job: { attempt: 2 } })
    void jobs.runJob(purgeExpiredSessionsTask.implement(() => {}))
    void jobs.runJob(
      reportTask.implement(() => {}),
      { id: 1 },
    )
    void jobs.runJob(
      reportTask.implement(() => {}),
      { id: 1 },
      { job: { scope: "b" } },
    )
    // @ts-expect-error The task needs a payload.
    void jobs.runJob(sendEmail)
    // @ts-expect-error The payload has the type of the task.
    void jobs.runJob(sendEmail, { to: 1 })
  })

  test("runJob takes the arguments of a wrapper generic over the payload without a cast", () => {
    const jobs = createTestJobs({ scope: "a" })
    const runTestJob = <P>(
      implementation: TaskImplementation<string, P>,
      payload: P,
      job: RunJobInfo = {},
    ) => jobs.runJob(implementation, payload, { job })
    void runTestJob(
      reportTask.implement(() => {}),
      { id: 1 },
    )
    void runTestJob(
      sendEmailTask.implement(() => {}),
      { to: "a" },
    )
  })

  test("runFailure takes an implementation with onFailure, the payload of its task, and an optional scope for a scoped task", () => {
    const jobs = createTestJobs({ scope: "a" })
    const sendEmail = sendEmailTask.implement(() => {}, { onFailure: () => {} })
    void jobs.runFailure(sendEmail, { to: "a" }, { reason: "terminal-error", job: { attempt: 1 } })
    void jobs.runFailure(purgeExpiredSessionsTask.implement(() => {}, { onFailure: () => {} }))
    void jobs.runFailure(
      reportTask.implement(() => {}, { onFailure: () => {} }),
      { id: 1 },
    )
    const withoutFailure = sendEmailTask.implement(() => {})
    // @ts-expect-error The implementation needs onFailure.
    void jobs.runFailure(withoutFailure, { to: "a" })
    // @ts-expect-error The task needs a payload.
    void jobs.runFailure(sendEmail)
    // @ts-expect-error The payload has the type of the task.
    void jobs.runFailure(sendEmail, { to: 1 })
  })

  test("TestJobCall and TestJobQueries take their type arguments by default", () => {
    const check = (queries: TestJobQueries) => queries.payloadsFor(sendEmailTask)
    expectTypeOf(check).returns.toEqualTypeOf<{ to: string }[]>()
    expectTypeOf<TestJobCall["payload"]>().toEqualTypeOf<unknown>()
  })
})
