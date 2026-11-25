// UTC minute calendars shared by text and structured schedule definitions.

const weekdays = ["sun", "mon", "tue", "wed", "thu", "fri", "sat"] as const

/** English weekday name, starting with Sunday. */
export type Weekday = (typeof weekdays)[number]

/** Inclusive range or step through a field, starting at its lower bound by default. */
export type CronRange<V> =
  | { from: V; to: V; every?: number | undefined }
  | { every: number; from?: V | undefined; to?: V | undefined }

/** Values selected in one calendar field. */
export type CronField<V = number> = "*" | V | CronRange<V> | readonly (V | CronRange<V>)[]

/** UTC calendar with a required minute and wildcard defaults for the remaining fields. */
export interface CronDefinition {
  minute: CronField
  hour?: CronField | undefined
  dayOfMonth?: CronField | undefined
  month?: CronField | undefined
  dayOfWeek?: CronField<number | Weekday> | undefined
}

const fields = ["minute", "hour", "dayOfMonth", "month", "dayOfWeek"] as const
const bounds = [
  [0, 59],
  [0, 23],
  [1, 31],
  [1, 12],
  [0, 6],
] as const

type Selection = { from?: string | number; to?: string | number; every?: number }

/** Reads a string item as the same inclusive selection as a structured range. */
function parseSelection(text: string): Selection {
  const match = /^(?:(\*)|([a-z]+|\d+)(?:-([a-z]+|\d+))?)(?:\/([1-9]\d*))?$/i.exec(text)
  if (!match || (match[4] && match[2] && !match[3])) {
    throw new RangeError(`Invalid cron selection: ${text}`)
  }
  const selection: Selection = {}
  if (match[2]) {
    selection.from = match[2]
    selection.to = match[3] ?? match[2]
  }
  if (match[4]) {
    selection.every = Number(match[4])
  }
  return selection
}

function structuredSelection(item: unknown): Selection {
  if (item === "*") {
    return {}
  }
  if (
    typeof item === "number" ||
    (typeof item === "string" && weekdays.includes(item as Weekday))
  ) {
    return { from: item, to: item }
  }
  if (!item || typeof item !== "object") {
    throw new TypeError(`Invalid structured cron selection: ${String(item)}`)
  }
  const unknownKey = Object.keys(item).find(key => !["from", "to", "every"].includes(key))
  if (unknownKey !== undefined) {
    throw new TypeError(`Unknown cron range field: ${unknownKey}`)
  }
  const range = item as Selection
  if (range.every === undefined && (range.from === undefined || range.to === undefined)) {
    throw new TypeError("A cron range needs from and to")
  }
  return range
}

function compileField(selections: Selection[], index: number): number[] {
  const [min, max] = bounds[index]!
  const weekday = index === 4
  const result = new Set<number>()
  function endpoint(token: string | number): number {
    const named =
      weekday && typeof token === "string" ? weekdays.indexOf(token.toLowerCase() as Weekday) : -1
    if (named >= 0) {
      return named
    }
    if (typeof token !== "number" && (typeof token !== "string" || !/^\d+$/.test(token))) {
      throw new RangeError(`Invalid ${fields[index]}: ${token}`)
    }
    const value = Number(token)
    if (!Number.isInteger(value) || value < min || value > max + Number(weekday)) {
      throw new RangeError(`Invalid ${fields[index]}: ${token}`)
    }
    return value
  }
  for (const { from, to, every: step = 1 } of selections) {
    const start = from === undefined ? min : endpoint(from)
    const end = to === undefined ? max : endpoint(to)
    if (start > end || !Number.isSafeInteger(step) || step <= 0) {
      throw new RangeError(`Invalid range or step for ${fields[index]}`)
    }
    for (let value = start; value <= end; value += step) {
      result.add(weekday && value === 7 ? 0 : value)
    }
  }
  if (!result.size) {
    throw new RangeError(`Empty ${fields[index]}`)
  }
  return [...result].sort((a, b) => a - b)
}

/** Compiles either notation to five sorted numeric sets in calendar order, rejecting a calendar that never matches. */
export function compileCron(cron: string | CronDefinition): number[][] {
  const calendar = parseCron(cron)
  if (!hasDay(calendar)) {
    throw new RangeError("The cron calendar selects no existing day, such as February 30")
  }
  return calendar
}

function parseCron(cron: string | CronDefinition): number[][] {
  if (typeof cron === "string") {
    const parts = cron.trim().split(/\s+/)
    if (parts.length !== 5) {
      throw new TypeError("A cron expression needs five fields")
    }
    return parts.map((part, index) => compileField(part.split(",").map(parseSelection), index))
  }
  if (!cron || cron.minute === undefined) {
    throw new TypeError("A cron definition needs a minute field")
  }
  const unknownField = Object.keys(cron).find(key => !fields.includes(key as keyof CronDefinition))
  if (unknownField !== undefined) {
    throw new TypeError(`Unknown cron field: ${unknownField}`)
  }
  return fields.map((key, index) => {
    const { [key]: field = "*" } = cron
    const items = Array.isArray(field) ? field : [field]
    return compileField(items.map(structuredSelection), index)
  })
}

/** Whether a selected month has a selected day of month, counting February 29, unless weekdays alone decide the day. */
function hasDay(calendar: number[][]): boolean {
  const [, , days, months, weekdays] = calendar as [
    number[],
    number[],
    number[],
    number[],
    number[],
  ]
  // Every month has every weekday, so a restricted weekday field matches whether the day field adds to it or is a wildcard.
  if (weekdays.length < 7) {
    return true
  }
  const monthLengths = [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
  return months.some(month => days[0]! <= monthLengths[month - 1]!)
}

/** Checks for a matching UTC minute in a closed interval, visiting days rather than every missed minute. */
export function hasOccurrence(calendar: number[][], earliest: number, latest: number): boolean {
  const [minutes, hours, days, months, weekdays] = calendar as [
    number[],
    number[],
    number[],
    number[],
    number[],
  ]
  const dayMs = 86_400_000
  // The Gregorian calendar repeats every 400 years, so this span holds a match if any earlier one exists; February 29 alone can be eight years apart.
  const first = Math.max(earliest, latest - 146097 * dayMs)
  for (let day = Math.floor(latest / dayMs) * dayMs; day + dayMs > first; day -= dayMs) {
    const date = new Date(day)
    if (!months.includes(date.getUTCMonth() + 1)) {
      continue
    }
    const matchesDate = days.includes(date.getUTCDate())
    const matchesWeekday = weekdays.includes(date.getUTCDay())
    // Full sets behave like wildcards, including those spelled out as ranges or lists.
    const matchesDay =
      days.length < 31 && weekdays.length < 7
        ? matchesDate || matchesWeekday
        : matchesDate && matchesWeekday
    if (!matchesDay) {
      continue
    }
    for (let hourIndex = hours.length - 1; hourIndex >= 0; hourIndex--) {
      for (let minuteIndex = minutes.length - 1; minuteIndex >= 0; minuteIndex--) {
        const at = day + (hours[hourIndex]! * 60 + minutes[minuteIndex]!) * 60000
        if (at < first) {
          return false
        }
        if (at <= latest) {
          return true
        }
      }
    }
  }
  return false
}
