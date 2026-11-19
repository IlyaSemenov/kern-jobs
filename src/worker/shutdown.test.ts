// Stop of a worker at a termination signal: the check of the options of the helpers, the window of a repeated signal and the life of the listeners on a fake worker, also in a process that gets a late real signal, and processes whose stop and cleanup resolve, reject, hang, or wait until forced; the scenarios of a real worker are in `faults/`.
import { afterEach, describe, expect, test } from "bun:test"
import { mkdtempSync, rmSync, writeFileSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"

import type { ShutdownConfig } from "./faults/shutdown"
import { maxInterval, sleep } from "./loop"
import type { StopOptions, Worker } from "./runtime"
import {
  listenForStopSignals,
  signalWindow,
  stopOnSignals,
  type StopOnSignalsOptions,
  waitForSignalAndStop,
  type WaitForSignalAndStopOptions,
} from "./shutdown"

/** Counts of the listeners of SIGTERM and SIGINT. */
const listeners = () => [process.listenerCount("SIGTERM"), process.listenerCount("SIGINT")]

describe("options", () => {
  test("each helper throws for a wrong option before it listens for the signals", () => {
    const worker: Pick<Worker, "stop"> = { stop: () => Promise.resolve() }
    const before = listeners()
    const helpers: [string, (options: unknown) => unknown][] = [
      ["stopOnSignals", options => stopOnSignals(worker, options as StopOnSignalsOptions)],
      [
        "waitForSignalAndStop",
        options => waitForSignalAndStop(worker, options as WaitForSignalAndStopOptions),
      ],
    ]
    for (const [name, install] of helpers) {
      const cases: [unknown, ErrorConstructor, string][] = [
        [null, TypeError, `The options of ${name} must be an object`],
        [
          { abortAfter: -1 },
          RangeError,
          `abortAfter must be a number of seconds from 0 up to ${maxInterval}, got -1`,
        ],
        [{ timeout: maxInterval + 1 }, RangeError, "timeout"],
        [{ timeout: "25" }, RangeError, "timeout"],
      ]
      for (const [options, type, message] of cases) {
        expect(() => install(options)).toThrow(type)
        expect(() => install(options)).toThrow(message)
      }
    }
    const withClose = () =>
      stopOnSignals(worker, { close: "sql.end" } as unknown as StopOnSignalsOptions)
    expect(withClose).toThrow(TypeError)
    expect(withClose).toThrow("close must be a function")
    expect(listeners()).toEqual(before)
  })
})

describe("listeners", () => {
  /** Listeners of the application that a test adds, removed after it. */
  const added: (() => void)[] = []
  afterEach(() => {
    for (const listener of added.splice(0)) process.off("SIGTERM", listener)
  })

  /** Fake worker whose stop waits until the test settles it or a call forces it, recording the options of each call. */
  function createFakeWorker() {
    const calls: (StopOptions | undefined)[] = []
    const stopped = Promise.withResolvers<void>()
    const worker: Pick<Worker, "stop"> = {
      stop(options) {
        calls.push(options)
        if (options?.force) stopped.reject(new Error("forced"))
        return stopped.promise
      },
    }
    return { worker, calls, stopped }
  }

  test("the first signal stops the worker with the options, a signal before 2 s joins the stop, and one at 2 s forces it, which rejects, and removes the listeners", async () => {
    const { worker, calls } = createFakeWorker()
    const before = listeners()
    let now = 0
    const outcome = listenForStopSignals(worker, { abortAfter: 1, timeout: 3 }, { now: () => now })
    process.emit("SIGTERM")
    now = 1999
    process.emit("SIGINT")
    expect(calls).toEqual([{ abortAfter: 1, timeout: 3 }])
    now = 2000
    process.emit("SIGTERM")
    expect(calls).toEqual([{ abortAfter: 1, timeout: 3 }, { force: true }])
    expect(listeners()).toEqual(before)
    await expect(outcome).rejects.toThrow("forced")
  })

  test("a stop that settles 2 s after the first signal removes the listeners at once", async () => {
    const { worker, stopped } = createFakeWorker()
    const before = listeners()
    let now = 0
    const outcome = listenForStopSignals(worker, {}, { now: () => now })
    process.emit("SIGTERM")
    now = 2000
    stopped.resolve()
    await outcome
    expect(listeners()).toEqual(before)
  })

  test("a stop that throws rejects the outcome, and the listeners are removed once the window has passed", async () => {
    const failure = new Error("stop failed")
    const worker: Pick<Worker, "stop"> = {
      stop() {
        throw failure
      },
    }
    const before = listeners()
    let now = 0
    const outcome = listenForStopSignals(worker, {}, { now: () => now })
    process.emit("SIGTERM")
    now = 2000
    await expect(outcome).rejects.toBe(failure)
    expect(listeners()).toEqual(before)
  })

  test("a force that throws rejects the outcome with its error, while the first stop runs", async () => {
    const failure = new Error("force failed")
    const worker: Pick<Worker, "stop"> = {
      stop(options) {
        if (options?.force) throw failure
        return new Promise(() => {})
      },
    }
    let now = 0
    const outcome = listenForStopSignals(worker, {}, { now: () => now })
    process.emit("SIGTERM")
    now = 2000
    process.emit("SIGTERM")
    await expect(outcome).rejects.toBe(failure)
  })

  test("the window ends by the clock, also when its timer runs before the clock reaches 2 s", async () => {
    const { worker, calls } = createFakeWorker()
    let now = 0
    const outcome = listenForStopSignals(worker, {}, { now: () => now })
    process.emit("SIGTERM")
    // The timer of the window has run, and the clock has not moved.
    await sleep(signalWindow / 1000 + 0.1)
    now = 1999
    process.emit("SIGINT")
    expect(calls).toEqual([{}])
    now = 2000
    process.emit("SIGINT")
    expect(calls).toEqual([{}, { force: true }])
    await expect(outcome).rejects.toThrow("forced")
  }, 10_000)

  test("a signal after the window that finds the stop settled before the timer of the window has run only removes the listeners and leaves the process running", async () => {
    const proc = Bun.spawn(["bun", join(import.meta.dir, "faults", "late-signal.ts")], {
      stdout: "pipe",
      stderr: "inherit",
    })
    procs.push(proc)
    const [code, stdout] = await Promise.all([proc.exited, new Response(proc.stdout).text()])
    expect([code, proc.signalCode]).toEqual([0, null])
    expect(JSON.parse(stdout)).toEqual({ calls: 1, listeners: [0, 0] })
  })

  test("waitForSignalAndStop reads only its own options, resolves once the stop resolves, and its listeners ignore a repeat until 2 s after the first signal, then leave only those of the application", async () => {
    const { worker, calls, stopped } = createFakeWorker()
    const applicationListener = () => {}
    process.on("SIGTERM", applicationListener)
    added.push(applicationListener)
    const before = listeners()
    // Settings of the application may carry a `close` of its own.
    const options = {
      abortAfter: 1,
      get close(): never {
        throw new Error("close was read")
      },
    }
    const outcome = waitForSignalAndStop(worker, options)
    expect(listeners()).toEqual(before.map(count => count + 1))
    process.emit("SIGTERM")
    stopped.resolve()
    await outcome
    process.emit("SIGINT")
    expect(calls).toEqual([{ abortAfter: 1, timeout: undefined }])
    expect(listeners()).toEqual(before.map(count => count + 1))
    await sleep(signalWindow / 1000 + 0.1)
    expect(listeners()).toEqual(before)
  }, 10_000)
})

/** Processes of the tests, which the teardown kills, since a regression can leave one waiting forever, and their temporary directories. */
const procs: Bun.Subprocess[] = []
const dirs: string[] = []
afterEach(() => {
  for (const proc of procs.splice(0)) proc.kill(9)
  for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true })
})

/** Starts the process of `faults/shutdown.ts`, which prints each phase on a line, and returns it with a wait for a line and for its end. */
function spawnShutdown(cfg: ShutdownConfig) {
  const proc = Bun.spawn(["bun", join(import.meta.dir, "faults", "shutdown.ts")], {
    env: { ...process.env, SHUTDOWN_CONFIG: JSON.stringify(cfg) },
    stdout: "pipe",
    stderr: "pipe",
  })
  procs.push(proc)
  const lines: string[] = []
  let done = false
  let waiters: (() => void)[] = []
  const read = (async () => {
    const decoder = new TextDecoder()
    let buffer = ""
    for await (const chunk of proc.stdout) {
      buffer += decoder.decode(chunk, { stream: true })
      const parts = buffer.split("\n")
      buffer = parts.pop()!
      lines.push(...parts)
      for (const wake of waiters.splice(0)) wake()
    }
    done = true
    for (const wake of waiters.splice(0)) wake()
  })()
  const stderr = new Response(proc.stderr).text()
  return {
    proc,
    /** Waits until the process has printed the line. */
    async waitForLine(line: string) {
      while (!lines.includes(line)) {
        if (done) throw new Error(`the process ended before "${line}"; lines: ${lines.join(", ")}`)
        await new Promise<void>(resolve => waiters.push(resolve))
      }
    },
    /** Waits until the process has ended, and returns its exit code, the signal that ended it, its lines, and its error output. */
    async ended() {
      await proc.exited
      await read
      return { code: proc.exitCode, signal: proc.signalCode, lines, stderr: await stderr }
    },
  }
}

describe("waitForSignalAndStop", () => {
  test("returns control to the application once the stop resolves, and a repeat of the signal during the cleanup within 2 s does not end the process", async () => {
    const dir = mkdtempSync(join(tmpdir(), "kern-jobs-shutdown-"))
    dirs.push(dir)
    const gateFile = join(dir, "gate")
    const p = spawnShutdown({
      helper: "waitForSignalAndStop",
      stop: "resolve",
      close: "gate",
      gateFile,
    })
    await p.waitForLine("listening")
    p.proc.kill("SIGINT")
    await p.waitForLine("close")
    p.proc.kill("SIGINT")
    // The repeat reaches the process before its cleanup ends.
    await sleep(0.2)
    writeFileSync(gateFile, "")
    const { code, signal, lines } = await p.ended()
    expect([code, signal, lines]).toEqual([0, null, ["listening", "stop", "close", "closed"]])
  }, 10_000)

  test("a signal after 2 s forces a stop in progress, which rejects, the application cleans up, and the next signal ends the process by its default action", async () => {
    const p = spawnShutdown({ helper: "waitForSignalAndStop", stop: "until-forced", close: "hang" })
    await p.waitForLine("listening")
    p.proc.kill("SIGTERM")
    await p.waitForLine("stop")
    await sleep(signalWindow / 1000 + 0.1)
    p.proc.kill("SIGTERM")
    await p.waitForLine("close")
    p.proc.kill("SIGTERM")
    const { signal, lines, stderr } = await p.ended()
    expect([signal, lines]).toEqual(["SIGTERM", ["listening", "stop", "force", "close"]])
    expect(stderr).toContain("The worker stop was forced")
  }, 10_000)

  test("once the stop has resolved and 2 s have passed, a signal ends the process during the cleanup by its default action", async () => {
    const p = spawnShutdown({ helper: "waitForSignalAndStop", stop: "resolve", close: "hang" })
    await p.waitForLine("listening")
    p.proc.kill("SIGINT")
    await p.waitForLine("close")
    await sleep(signalWindow / 1000 + 0.1)
    p.proc.kill("SIGINT")
    const { signal, lines } = await p.ended()
    expect([signal, lines]).toEqual(["SIGINT", ["listening", "stop", "close"]])
  }, 10_000)
})

describe("stopOnSignals", () => {
  /** Runs a process that sends itself SIGTERM, and returns its exit code, lines, and error output. */
  async function runSelfSignaled(cfg: Omit<ShutdownConfig, "helper" | "selfSignal">) {
    const { code, lines, stderr } = await spawnShutdown({
      ...cfg,
      helper: "stopOnSignals",
      selfSignal: true,
    }).ended()
    return { code, lines, stderr }
  }

  test("a stop and a close that succeed exit the process with 0", async () => {
    const { code, lines, stderr } = await runSelfSignaled({
      stop: "resolve",
      close: "resolve",
      timeout: 5,
    })
    expect([code, lines, stderr]).toEqual([0, ["listening", "stop", "close"], ""])
  })

  test("a failed stop still closes the client, and the process exits with 1", async () => {
    const { code, lines, stderr } = await runSelfSignaled({
      stop: "reject",
      close: "resolve",
      timeout: 5,
    })
    expect([code, lines]).toEqual([1, ["listening", "stop", "close"]])
    expect([stderr.startsWith("kern-jobs: stop error:"), stderr.includes("stop failed")]).toEqual([
      true,
      true,
    ])
  })

  test("a stop that throws still closes the client, and the process exits with 1", async () => {
    const { code, lines, stderr } = await runSelfSignaled({
      stop: "throw",
      close: "resolve",
      timeout: 5,
    })
    expect([code, lines]).toEqual([1, ["listening", "stop", "close"]])
    expect([stderr.startsWith("kern-jobs: stop error:"), stderr.includes("stop failed")]).toEqual([
      true,
      true,
    ])
  })

  test("a failed close exits the process with 1", async () => {
    const { code, lines, stderr } = await runSelfSignaled({
      stop: "resolve",
      close: "reject",
      timeout: 5,
    })
    expect([code, lines]).toEqual([1, ["listening", "stop", "close"]])
    expect([stderr.startsWith("kern-jobs: close error:"), stderr.includes("close failed")]).toEqual(
      [true, true],
    )
  })

  test("a close that hangs after the stop exits the process with 1 at the timeout", async () => {
    const { code, lines, stderr } = await runSelfSignaled({
      stop: "resolve",
      close: "hang",
      timeout: 0.3,
    })
    expect([code, lines]).toEqual([1, ["listening", "stop", "close"]])
    expect(stderr).toContain("kern-jobs: shutdown did not finish within 0.3 s; exiting")
  })

  test("a signal after 2 s forces a stop in progress, and the process closes the client and exits with 1", async () => {
    const p = spawnShutdown({
      helper: "stopOnSignals",
      stop: "until-forced",
      close: "resolve",
      timeout: 10,
    })
    await p.waitForLine("listening")
    p.proc.kill("SIGTERM")
    await p.waitForLine("stop")
    await sleep(signalWindow / 1000 + 0.1)
    p.proc.kill("SIGINT")
    const { code, lines, stderr } = await p.ended()
    expect([code, lines]).toEqual([1, ["listening", "stop", "force", "close"]])
    expect(stderr).toContain("The worker stop was forced")
  }, 10_000)
})
