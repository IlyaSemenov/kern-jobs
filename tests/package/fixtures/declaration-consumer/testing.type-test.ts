import { describe, expectTypeOf, test } from "bun:test"

import type { Jobs } from "kern-jobs"
import type {
  RunFailureArgs,
  RunFailureJob,
  RunFailureOptions,
  TestJobCall,
  TestJobQueries,
} from "kern-jobs/testing"

import { indexProductTask } from "../declarations/tasks.js"
import { captureJobs, runIndex, runIndexFailure, testJobs } from "../declarations/testing.js"

describe("emitted declarations of test helpers", () => {
  test("keep the jobs API of tests, the queries of a capture, the result of a run, and the options of a run of onFailure", async () => {
    expectTypeOf(testJobs).toExtend<Jobs>()
    expectTypeOf(testJobs.calls).toEqualTypeOf<readonly TestJobCall[]>()
    const captured = await captureJobs(() => 1)
    expectTypeOf(captured).toEqualTypeOf<TestJobQueries<number>>()
    expectTypeOf(captured.callsFor(indexProductTask)).toEqualTypeOf<
      TestJobCall<{ productId: string }>[]
    >()
    expectTypeOf(runIndex("1")).toEqualTypeOf<Promise<unknown>>()
    expectTypeOf(runIndexFailure).parameter(1).toEqualTypeOf<RunFailureOptions | undefined>()
    expectTypeOf<RunFailureOptions["job"]>().toEqualTypeOf<RunFailureJob | undefined>()
    expectTypeOf<[productId: string]>().toExtend<RunFailureArgs<string, string | null>>()
  })
})
