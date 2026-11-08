// Stop of a worker process at a termination signal: the check of its options, and processes whose stop or close resolves, rejects, or hangs; the scenarios of a real worker are in `faults/`.
import { expect, test } from "bun:test"
import { join } from "node:path"

import type { ShutdownConfig } from "./faults/shutdown"
import { maxInterval } from "./loop"
import type { Worker } from "./runtime"
import { stopOnSignals, type StopOnSignalsOptions } from "./shutdown"

test("throws for a wrong option before it listens for the signals", () => {
  const worker: Pick<Worker, "stop"> = { stop: () => Promise.resolve() }
  const listeners = () => [process.listenerCount("SIGTERM"), process.listenerCount("SIGINT")]
  const before = listeners()
  const cases: [unknown, ErrorConstructor, string][] = [
    [null, TypeError, "The options of stopOnSignals must be an object"],
    [
      { abortAfter: -1 },
      RangeError,
      `abortAfter must be a number of seconds from 0 up to ${maxInterval}, got -1`,
    ],
    [{ timeout: maxInterval + 1 }, RangeError, "timeout"],
    [{ timeout: "25" }, RangeError, "timeout"],
    [{ close: "sql.end" }, TypeError, "close must be a function"],
  ]
  for (const [options, type, message] of cases) {
    const install = () => stopOnSignals(worker, options as StopOnSignalsOptions)
    expect(install).toThrow(type)
    expect(install).toThrow(message)
  }
  expect(listeners()).toEqual(before)
})

/** Runs the process of `faults/shutdown.ts` and returns its exit code and output. */
async function runShutdown(cfg: ShutdownConfig) {
  const proc = Bun.spawn(["bun", join(import.meta.dir, "faults", "shutdown.ts")], {
    env: { ...process.env, SHUTDOWN_CONFIG: JSON.stringify(cfg) },
    stdout: "pipe",
    stderr: "pipe",
  })
  const [code, stdout, stderr] = await Promise.all([
    proc.exited,
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
  ])
  return { code, calls: stdout.trim().split("\n"), stderr }
}

test("a stop and a close that succeed exit the process with 0", async () => {
  const { code, calls, stderr } = await runShutdown({
    stop: "resolve",
    close: "resolve",
    timeout: 5,
  })
  expect([code, calls, stderr]).toEqual([0, ["stop", "close"], ""])
})

test("a failed stop still closes the client, and the process exits with 1", async () => {
  const { code, calls, stderr } = await runShutdown({
    stop: "reject",
    close: "resolve",
    timeout: 5,
  })
  expect([code, calls]).toEqual([1, ["stop", "close"]])
  expect([stderr.startsWith("kern-jobs: stop error:"), stderr.includes("stop failed")]).toEqual([
    true,
    true,
  ])
})

test("a failed close exits the process with 1", async () => {
  const { code, calls, stderr } = await runShutdown({
    stop: "resolve",
    close: "reject",
    timeout: 5,
  })
  expect([code, calls]).toEqual([1, ["stop", "close"]])
  expect([stderr.startsWith("kern-jobs: close error:"), stderr.includes("close failed")]).toEqual([
    true,
    true,
  ])
})

test("a close that hangs after the stop exits the process with 1 at the timeout", async () => {
  const { code, calls, stderr } = await runShutdown({
    stop: "resolve",
    close: "hang",
    timeout: 0.3,
  })
  expect([code, calls]).toEqual([1, ["stop", "close"]])
  expect(stderr).toContain("kern-jobs: shutdown did not finish within 0.3 s; exiting")
})
