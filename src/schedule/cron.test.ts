import { expect, test } from "bun:test"

import { createRandom } from "../core/testing"

import { compileCron, hasOccurrence } from "./cron"

test("structured steps, ranges, duplicates and Sunday aliases normalize to the same sets", () => {
  expect(
    compileCron({
      minute: { every: 15 },
      hour: [3, 3, 8],
      dayOfWeek: [{ from: "mon", to: "fri", every: 2 }, "sun"],
    }),
  ).toEqual(compileCron("*/15 8,3 * * MON-FRI/2,7"))
})

test("rejects malformed calendars and requires an explicit minute", () => {
  for (const invalid of [
    {},
    { minute: [] },
    { minute: { every: 0 } },
    { minute: 60 },
    { minute: 0, weekday: 3 },
    "0 3 * * fri-mon",
    "0 3 * *",
    "0 3 * * 8",
  ])
    expect(() => compileCron(invalid as never)).toThrow()
})

test("restricted days use OR, while a full day field leaves the other restriction in force", () => {
  const monday = Date.UTC(2030, 0, 7)
  for (const expr of ["0 0 13 * mon", "0 0 * * mon"])
    expect(hasOccurrence(compileCron(expr), monday, monday)).toBe(true)
  expect(hasOccurrence(compileCron("0 0 13 * 0-7"), monday, monday)).toBe(false)
})

test("rejects a calendar without an existing day and accepts February 29 for leap years", () => {
  for (const impossible of ["0 0 30 2 *", "0 0 31 4,6 *", { minute: 0, dayOfMonth: 31, month: 2 }])
    expect(() => compileCron(impossible)).toThrow(
      new RangeError("The cron calendar selects no existing day, such as February 30"),
    )
  // A restricted weekday matches by itself, so the day of month cannot make the calendar empty.
  expect(() => compileCron("0 0 30 2 mon")).not.toThrow()
  const leapDay = compileCron("0 0 29 2 *")
  expect(hasOccurrence(leapDay, Date.UTC(2029, 0, 1), Date.UTC(2031, 11, 31))).toBe(false)
  expect(hasOccurrence(leapDay, Date.UTC(2029, 0, 1), Date.UTC(2032, 1, 29))).toBe(true)
})

test("finds the same minutes as a check of every minute, over random calendars and intervals", () => {
  const seed = Number(process.env.FUZZ_SEED ?? 1)
  const random = createRandom(seed)
  const domains = [
    [0, 59],
    [0, 23],
    [1, 31],
    [1, 12],
    [0, 7],
  ] as const
  /** Returns a valid field and its lowest value. */
  const randomField = ([min, max]: readonly [number, number]): [string, number] => {
    const value = () => random.int(min, max)
    switch (random.int(0, 4)) {
      case 0:
        return ["*", min]
      case 1: {
        const single = value()
        return [String(single), single]
      }
      case 2: {
        const [first, second] = [value(), value()]
        return [`${first},${second}`, Math.min(first, second)]
      }
      case 3: {
        const [from, to] = [value(), value()].sort((a, b) => a - b)
        return [`${from}-${to}`, from!]
      }
      default:
        return [`*/${random.int(2, 10)}`, min]
    }
  }
  /** Matches one minute by the cron rule directly, without the search of `hasOccurrence`. */
  const matchesMinute = (calendar: number[][], at: Date) => {
    const [minutes, hours, days, months, weekdays] = calendar as [
      number[],
      number[],
      number[],
      number[],
      number[],
    ]
    const date = days.includes(at.getUTCDate())
    const weekday = weekdays.includes(at.getUTCDay())
    return (
      minutes.includes(at.getUTCMinutes()) &&
      hours.includes(at.getUTCHours()) &&
      months.includes(at.getUTCMonth() + 1) &&
      (days.length < 31 && weekdays.length < 7 ? date || weekday : date && weekday)
    )
  }
  for (let round = 0; round < 300; round++) {
    const fields = domains.map(randomField)
    // January has every day of month, so it keeps a day field from the 30th valid.
    if (fields[2]![1] >= 30) fields[3] = ["*", 1]
    const expression = fields.map(([text]) => text).join(" ")
    let calendar: number[][]
    try {
      calendar = compileCron(expression)
    } catch (error) {
      throw new Error(`seed ${seed}, ${expression}`, { cause: error })
    }
    const earliest =
      Date.UTC(2030, 0, 1) + random.int(0, 3 * 365 * 1440) * 60000 + random.int(0, 59999)
    const latest = earliest + random.int(0, 3 * 1440) * 60000
    let expected = false
    for (let at = Math.ceil(earliest / 60000) * 60000; at <= latest && !expected; at += 60000)
      expected = matchesMinute(calendar, new Date(at))
    // The inputs beside the result show in a failure, which the seed replays.
    const input = { seed, expression, earliest: new Date(earliest), latest: new Date(latest) }
    expect({ ...input, found: hasOccurrence(calendar, earliest, latest) }).toEqual({
      ...input,
      found: expected,
    })
  }
})
