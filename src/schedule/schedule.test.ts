import { expect, mock, test } from "bun:test"

import { defineTask } from "../task/task"

import { defineSchedule, toWorkerSchedule } from "./schedule"

const task = defineTask("backup")

test("rejects an ID with a lone surrogate, which would share its row with another ID, and accepts a surrogate pair", () => {
  for (const id of ["\ud800", "\ud801", "a\udc00"])
    expect(() => defineSchedule(task, { id, cron: { minute: 0 } })).toThrow(
      new TypeError(
        "Schedule ID must be a string that is not empty and has neither U+0000 nor a lone surrogate",
      ),
    )
  expect(defineSchedule(task, { id: "backup-💾", cron: { minute: 0 } }).id).toBe("backup-💾")
})

test("checks the overrides of a schedule without calling the withOptions function of its task", () => {
  let calls = 0
  const debounced = defineTask("search.indexProduct", { key: "product" }).withOptions(() => {
    calls++
    return { keyMode: "debounce", delay: 1, maxWait: 5 }
  })
  for (const [enqueue, message] of [
    [{ maxAttempts: 0 }, "maxAttempts must be"],
    [{ key: "k\ud800" }, "key must be"],
    [{ delay: -1 }, "delay must be"],
    [{ keyMode: "debounce", maxWait: -1 }, "maxWait must be"],
    [{ keyMode: "debounce", runAt: new Date() }, 'keyMode "debounce" takes delay and maxWait'],
    // As a JavaScript caller passes them, past the types.
    [{ keyMode: "debounce", delay: null }, 'keyMode "debounce" takes delay and maxWait'],
    [{ keyMode: "debounce", maxWait: null }, 'keyMode "debounce" takes delay and maxWait'],
    // `runAt: null` drops the delay that the function gives.
    [{ keyMode: "debounce", runAt: null }, 'keyMode "debounce" takes delay and maxWait'],
    [{ delay: 1, runAt: new Date() }, "Pass either delay or runAt, not both"],
    [{ lease: 5 }, "lease, backoff, and scoped are settings of defineTask"],
  ] as const) {
    expect(() =>
      defineSchedule(debounced, { cron: { minute: 0 }, enqueue: enqueue as never }),
    ).toThrow(message)
  }
  // The overrides can inherit delay and maxWait, and their own delay survives their `runAt: null`.
  for (const enqueue of [{ keyMode: "debounce" }, { keyMode: "debounce", delay: 1, runAt: null }]) {
    expect(() =>
      defineSchedule(debounced, { cron: { minute: 0 }, enqueue: enqueue as never }),
    ).not.toThrow()
  }
  expect(calls).toBe(0)
})

test("rejects a schedule of a batch task with items of different keys of withKey", () => {
  const keyFor = mock((item: string) => item)
  const mixed = defineTask("digest").withBatchPayload<string>({ maxItems: 10 }).withKey(keyFor)
  expect(() => defineSchedule(mixed, { cron: { minute: 0 }, payload: ["a", "b"] })).toThrow(
    TypeError,
  )
  expect(keyFor).toHaveBeenCalled()
})

test("rejects a misspelled override of a schedule of a batch task", () => {
  const digest = defineTask("digest", { key: true }).withBatchPayload<string>({ maxItems: 10 })
  expect(() =>
    defineSchedule(digest, { cron: { minute: 0 }, payload: ["a"], enqueue: { kye: "k" } as never }),
  ).toThrow(
    new TypeError(
      "An enqueue of batch task digest takes only a key, got kye; its other options come from defineTask",
    ),
  )
})

test("reads a getter of the overrides once per check of a schedule, and gives the worker the value that it checked", () => {
  let reads = 0
  class Overrides {
    get key() {
      reads++
      return `k${reads}`
    }
  }
  const schedule = defineSchedule(task, { cron: { minute: 0 }, enqueue: new Overrides() })
  const workerSchedule = toWorkerSchedule(schedule)
  expect([reads, workerSchedule.enqueue]).toEqual([2, { key: "k2" }])
})
