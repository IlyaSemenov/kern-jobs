// Loop: the range of an interval, rounds never overlap, a wake starts the next round, and stop waits for the running round.
import { expect, test } from "bun:test"

import { isInterval, maxInterval, startLoop } from "./loop"

test("an interval is a number of seconds above 0 that one timer waits", () => {
  // The longest delay of a timer, 2^31 - 1 milliseconds, in whole seconds.
  expect(maxInterval).toBe(Math.floor((2 ** 31 - 1) / 1000))
  const valid = [0.001, 1, maxInterval]
  const invalid = [0, -1, maxInterval + 1, Infinity, NaN, "1", null, undefined]
  expect(valid.map(isInterval)).toEqual([true, true, true])
  expect(invalid.filter(isInterval)).toEqual([])
})

test("rounds never overlap, however often the loop is woken", async () => {
  let running = 0
  let maxRunning = 0
  let rounds = 0
  const loop = startLoop(
    async () => {
      running++
      maxRunning = Math.max(maxRunning, running)
      rounds++
      await new Promise(r => setTimeout(r, 10))
      running--
    },
    { interval: 0, onError: () => {} },
  )
  for (let i = 0; i < 20; i++) {
    loop.wake()
    await new Promise(r => setTimeout(r, 2))
  }
  await loop.stop()
  expect(maxRunning).toBe(1)
  expect(rounds).toBeGreaterThan(1)
})

test("a wake during a round starts the next round right after it, without the interval", async () => {
  const gate = Promise.withResolvers<void>()
  let rounds = 0
  const second = Promise.withResolvers<void>()
  const loop = startLoop(
    async () => {
      rounds++
      if (rounds === 1) await gate.promise
      if (rounds === 2) second.resolve()
    },
    { interval: 3600, onError: () => {} },
  )
  loop.wake()
  gate.resolve()
  await second.promise
  await loop.stop()
  expect(rounds).toBe(2)
})

test("a wake during the pause starts the next round", async () => {
  let rounds = 0
  const loop = startLoop(
    async () => {
      rounds++
    },
    { interval: 3600, onError: () => {} },
  )
  await new Promise(r => setTimeout(r, 10))
  loop.wake()
  await new Promise(r => setTimeout(r, 10))
  await loop.stop()
  expect(rounds).toBe(2)
})

test("a round that throws is reported, and the loop goes on", async () => {
  const errors: unknown[] = []
  let rounds = 0
  const loop = startLoop(
    async () => {
      if (++rounds === 1) throw new Error("boom")
    },
    { interval: 0, onError: e => errors.push(e) },
  )
  while (rounds < 2) await new Promise(r => setTimeout(r, 1))
  await loop.stop()
  expect(errors.map(e => (e as Error).message)).toEqual(["boom"])
})

test("stop waits for the running round and starts no other", async () => {
  const gate = Promise.withResolvers<void>()
  let rounds = 0
  let finished = false
  const loop = startLoop(
    async () => {
      rounds++
      await gate.promise
      finished = true
    },
    { interval: 0, onError: () => {} },
  )
  const stopped = loop.stop()
  gate.resolve()
  await stopped
  expect([rounds, finished]).toEqual([1, true])
})
