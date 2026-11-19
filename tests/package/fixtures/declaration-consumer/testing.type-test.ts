import { describe, expectTypeOf, test } from "bun:test"

import type { Jobs } from "kern-jobs"
import type { TestJobCall, TestJobQueries } from "kern-jobs/testing"

import { indexProductTask } from "../declarations/tasks.js"
import { captureJobs, runIndex, testJobs } from "../declarations/testing.js"

describe("emitted declarations of test helpers", () => {
  test("keep the jobs API of tests, the queries of a capture, and the result of a run", async () => {
    expectTypeOf(testJobs).toExtend<Jobs>()
    expectTypeOf(testJobs.calls).toEqualTypeOf<readonly TestJobCall[]>()
    const captured = await captureJobs(() => 1)
    expectTypeOf(captured).toEqualTypeOf<TestJobQueries<number>>()
    expectTypeOf(captured.callsFor(indexProductTask)).toEqualTypeOf<
      TestJobCall<{ productId: string }>[]
    >()
    expectTypeOf(runIndex("1")).toEqualTypeOf<Promise<unknown>>()
  })
})
