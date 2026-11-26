import { describe, expectTypeOf, test } from "bun:test"

import type {
  Backoff,
  FailureImplementation,
  JobFailure,
  JobInfo,
  Jobs,
  TaskBuilder,
  TaskImplementation,
} from "kern-jobs"
import type { AbortReason, CronDefinition, Manifest, Schedule, Worker } from "kern-jobs/worker"

import {
  defineCleanup,
  indexBackoff,
  indexProductTask,
  purgeExpiredSessionsTask,
} from "../declarations/tasks.js"
import {
  createCleanupSchedule,
  manifest,
  unimplementedTasks,
  weeklyCleanup,
} from "../declarations/worker/manifest.js"
import {
  describeAbort,
  describeJob,
  startConsumerWorker,
  stopConsumerWorkerAtSignal,
} from "../declarations/worker/start.js"
import indexProduct from "../declarations/worker/tasks/index-product.js"
import purgeExpiredSessions from "../declarations/worker/tasks/purge-expired-sessions.js"

declare const jobs: Jobs

describe("emitted declarations of the worker", () => {
  test("keep the task and the payload of each implementation, the manifest, a schedule, and the metadata of a job", () => {
    expectTypeOf(indexProduct).toEqualTypeOf<
      TaskImplementation<
        "search.indexProduct",
        { productId: string },
        string | null,
        typeof indexProductTask
      > & {
        readonly failure: FailureImplementation<"search.indexProduct", { productId: string }>
      }
    >()
    expectTypeOf(indexProduct.failure.task.name).toEqualTypeOf<"search.indexProduct:failure">()
    expectTypeOf(indexProduct.failure.handler)
      .parameter(0)
      .toEqualTypeOf<JobFailure<{ productId: string }>>()
    expectTypeOf(purgeExpiredSessions).toEqualTypeOf<
      TaskImplementation<
        "sessions.purgeExpired",
        void,
        string | null,
        typeof purgeExpiredSessionsTask
      >
    >()
    expectTypeOf(manifest).toEqualTypeOf<Manifest>()
    expectTypeOf(weeklyCleanup).toEqualTypeOf<Schedule<void>>()
    expectTypeOf(weeklyCleanup.cron).toEqualTypeOf<string | CronDefinition>()
    expectTypeOf(createCleanupSchedule).returns.toEqualTypeOf<Schedule<void>>()
    expectTypeOf(describeJob).returns.toEqualTypeOf<JobInfo>()
  })

  test("keep the worker, the outcome of its stop at a signal, the reason of an abort, and the check of the manifest", () => {
    expectTypeOf(startConsumerWorker).returns.toEqualTypeOf<Worker>()
    expectTypeOf(stopConsumerWorkerAtSignal).returns.toEqualTypeOf<Promise<void>>()
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
    // @ts-expect-error The handler of the failures of another task takes the failures of another payload.
    purgeExpiredSessionsTask.implement(() => {}, { onFailure: indexProduct.failure.handler })
    // @ts-expect-error An enqueue cannot override the lease.
    void jobs.enqueue(purgeExpiredSessionsTask, undefined, { lease: 5 })
  })
})
