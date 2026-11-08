import { describe, expectTypeOf, test } from "bun:test"

import type { Backoff, JobInfo, Jobs, TaskBuilder, TaskImplementation } from "kern-jobs"
import type { AbortReason, Manifest, Worker } from "kern-jobs/worker"

import {
  defineCleanup,
  indexBackoff,
  indexProductTask,
  purgeExpiredSessionsTask,
} from "../declarations/tasks.js"
import { manifest, unimplementedTasks } from "../declarations/worker/manifest.js"
import { describeAbort, describeJob, startConsumerWorker } from "../declarations/worker/start.js"
import indexProduct from "../declarations/worker/tasks/index-product.js"
import purgeExpiredSessions from "../declarations/worker/tasks/purge-expired-sessions.js"

declare const jobs: Jobs

describe("emitted declarations of the worker", () => {
  test("keep the task and the payload of each implementation, the manifest, and the metadata of a job", () => {
    expectTypeOf(indexProduct).toEqualTypeOf<
      TaskImplementation<"search.indexProduct", { productId: string }>
    >()
    expectTypeOf(purgeExpiredSessions).toEqualTypeOf<
      TaskImplementation<"sessions.purgeExpired", void>
    >()
    expectTypeOf(manifest).toEqualTypeOf<Manifest>()
    expectTypeOf(describeJob).returns.toEqualTypeOf<JobInfo>()
  })

  test("keep the worker, the reason of an abort, and the check of the manifest", () => {
    expectTypeOf(startConsumerWorker).returns.toEqualTypeOf<Worker>()
    expectTypeOf(describeAbort).returns.toEqualTypeOf<AbortReason | null>()
    expectTypeOf(unimplementedTasks).toEqualTypeOf<string[]>()
  })

  test("keep the settings of a task", () => {
    expectTypeOf(indexBackoff).toEqualTypeOf<Backoff>()
    expectTypeOf(defineCleanup("sessions.purge")).toEqualTypeOf<
      TaskBuilder<"sessions.purge", void>
    >()
  })

  // Each call stays on one line, where every version of TypeScript reports its error.
  test("let implement reject a handler of another payload, and enqueue reject a setting", () => {
    // @ts-expect-error The handler takes the payload of the task.
    indexProductTask.implement((payload: { id: number }) => payload)
    // @ts-expect-error An enqueue cannot override the lease.
    void jobs.enqueue(purgeExpiredSessionsTask, undefined, { lease: 5 })
  })
})
