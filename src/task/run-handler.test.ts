// Run of the handler of a job through `runHandler`: the promise of `run`.

import { expect, test } from "bun:test"

import { runTaskHandler } from "./run-handler"

test("gives runHandler a run whose promise rejects also when the handler throws synchronously", async () => {
  const failure = new Error("the index is down")
  const implementation = {
    handler: () => {
      throw failure
    },
  }
  const context = {
    signal: new AbortController().signal,
    job: { id: "1", scope: null, attempt: 1, maxAttempts: 25, key: null, queue: null },
    task: "search.indexProduct",
    payload: undefined,
  }
  const result = runTaskHandler(implementation, context, (_context, run) => {
    // A synchronous throw would escape the `catch` of the promise and fail the test.
    return run().catch(error => error)
  })
  expect(await result).toBe(failure)
})
