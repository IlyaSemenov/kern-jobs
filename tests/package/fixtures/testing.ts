// Test helpers of the consumer, exported with inferred types that its declarations name through the exports of kern-jobs and kern-jobs/testing; the type tests of `declaration-consumer/` check the emitted types.

import { createTestJobs, runJob } from "kern-jobs/testing"

import { indexProductTask } from "./tasks.ts"

export const testJobs = createTestJobs({ scope: () => "tenant" })

/** Captures the enqueues of an action, as the tests of the consumer do. */
export function captureJobs<R>(action: () => R) {
  return testJobs.capture(action)
}

/** Runs the indexing of a product with the metadata of its job that a test gives. */
export function runIndex(productId: string, attempt?: number) {
  return runJob(
    indexProductTask.implement(() => {}),
    { productId },
    { job: { attempt } },
  )
}
