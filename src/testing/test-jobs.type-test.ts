import { describe, expectTypeOf, test } from "bun:test"

import type { Jobs } from "../producer/jobs"
import { defineTask } from "../task/task"

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

  test("TestJobCall and TestJobQueries take their type arguments by default", () => {
    const check = (queries: TestJobQueries) => queries.payloadsFor(sendEmailTask)
    expectTypeOf(check).returns.toEqualTypeOf<{ to: string }[]>()
    expectTypeOf<TestJobCall["payload"]>().toEqualTypeOf<unknown>()
  })
})
