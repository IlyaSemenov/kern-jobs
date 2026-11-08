import { describe, expectTypeOf, test } from "bun:test"

import type { EnqueueResult, Jobs, Task, TaskBuilder } from "kern-jobs"

import { createPgJobs, createPostgresJobs, enqueueIndex } from "../declarations/jobs.js"
import {
  indexProductTask,
  purgeExpiredSessionsTask,
  sendReminderTask,
} from "../declarations/tasks.js"

declare const jobs: Jobs

describe("emitted declarations of tasks", () => {
  test("keep the name and the payload of each task", () => {
    expectTypeOf(indexProductTask).toEqualTypeOf<
      Task<"search.indexProduct", { productId: string }>
    >()
    expectTypeOf(sendReminderTask).toEqualTypeOf<
      Task<"reminders.send", { reminderId: string; at?: string }>
    >()
    expectTypeOf(purgeExpiredSessionsTask).toEqualTypeOf<
      TaskBuilder<"sessions.purgeExpired", void>
    >()
  })

  // Each call stays on one line, where every version of TypeScript reports its error.
  test("let enqueue reject a missing or an extra payload, and delay with runAt", () => {
    // @ts-expect-error The task needs a payload.
    void jobs.enqueue(indexProductTask)
    // @ts-expect-error The task has no payload.
    void jobs.enqueue(purgeExpiredSessionsTask, { productId: "42" })
    // @ts-expect-error delay and runAt exclude each other.
    void jobs.enqueue(purgeExpiredSessionsTask, undefined, { delay: 5, runAt: new Date() })
  })
})

describe("emitted declarations of jobs", () => {
  test("keep the jobs API and the result of an enqueue", () => {
    expectTypeOf(createPgJobs).returns.toEqualTypeOf<Jobs>()
    expectTypeOf(createPostgresJobs).returns.toEqualTypeOf<Jobs>()
    expectTypeOf(enqueueIndex).returns.toEqualTypeOf<Promise<EnqueueResult>>()
  })
})
