// Options of an enqueue: what a task definition and an enqueue give, their merge, and the parameters of the core.
import { checkCount } from "../core/db"
import type { EnqueueParams } from "../core/enqueue"

import { delayRange, isDelay } from "./delay"

interface CommonOptions {
  /** Key of the pending job within its task, such as `product:42`; `null` enqueues the job without a key. */
  key?: string | null | undefined
  /** Queue that runs one job at a time, such as `account:42`; `null` enqueues the job without a queue. */
  queue?: string | null | undefined
  /** Attempts before the job fails for good. */
  maxAttempts?: number | undefined
}

type TimeOptions =
  | {
      /** Seconds from the enqueue to the run time, or, with `keyMode: "debounce"`, from the latest event. */
      delay?: number | undefined
      runAt?: undefined
    }
  | {
      /** Earliest time at which the job runs. */
      runAt?: Date | undefined
      delay?: undefined
    }

type KeyModeOptions =
  | {
      /** A pending job of the key takes the new payload and options, and keeps its attempts. */
      keyMode?: "replace" | undefined
      /** Resets the attempts of the pending job of the key. */
      resetAttempts?: boolean | undefined
      maxWait?: undefined
    }
  | {
      /** A pending job of the key stays as it is. */
      keyMode: "keep"
      resetAttempts?: undefined
      maxWait?: undefined
    }
  | {
      /**
       * The job is due `delay` seconds after the latest event of the key, but no later than `maxWait` seconds after the first, while the events pass the same `delay` and `maxWait`; a later event never makes the job due earlier.
       */
      keyMode: "debounce"
      delay: number
      /** Seconds from the first event of a debounce to the latest time at which this event makes the job due. */
      maxWait: number
      resetAttempts?: boolean | undefined
      runAt?: undefined
    }

type KeyModeOverrides =
  | Exclude<KeyModeOptions, { keyMode: "debounce" }>
  | {
      keyMode?: "debounce" | undefined
      maxWait?: number | undefined
      resetAttempts?: boolean | undefined
      runAt?: undefined
    }

/** Settings of a task, which only `defineTask` takes, rejected in the options of `withOptions` and in overrides. */
interface NoSettings {
  /** Set by `defineTask` only. */
  lease?: undefined
  /** Set by `defineTask` only. */
  backoff?: undefined
  /** Set by `defineTask` only. */
  scoped?: undefined
}

/** Options of the enqueues of a task that `defineTask` takes beside its settings. */
export type StaticEnqueueOptions = CommonOptions & TimeOptions & KeyModeOptions

/** Options of the enqueues of a task, which the function of `withOptions` returns. */
export type EnqueueOptions = StaticEnqueueOptions & NoSettings

/**
 * Options of one enqueue that replace those of its task: a field given as `undefined` keeps the field of the task, and giving `delay` or `runAt` drops the other.
 */
export type EnqueueOverrides = CommonOptions & TimeOptions & KeyModeOverrides & NoSettings

/** Merged layers of options, which only `toEnqueueParams` checks. */
interface MergedOptions {
  key?: string | null
  queue?: string | null
  maxAttempts?: number
  delay?: number
  runAt?: Date
  keyMode?: "replace" | "keep" | "debounce"
  resetAttempts?: boolean
  maxWait?: number
  /** Settings of the task, which a layer above `defineTask` can give only past the types. */
  lease?: unknown
  backoff?: unknown
  scoped?: unknown
}

/**
 * Merges layers of options, each replacing the fields of the layers before it, such as the options that a task gives to `defineTask`, those of its `withOptions`, and the overrides of an enqueue: a layer replaces the fields it gives other than `undefined`, and `delay` and `runAt` exclude each other, so a layer that gives one drops the other.
 */
export function mergeOptions(
  ...layers: (EnqueueOptions | EnqueueOverrides | undefined)[]
): MergedOptions {
  const merged: Record<string, unknown> = {}
  for (const layer of layers) {
    if (layer == null) continue
    // Only the field of an earlier layer is dropped, so that `delay` and `runAt` of one layer reach the check.
    if (layer.delay !== undefined) delete merged.runAt
    if (layer.runAt !== undefined) delete merged.delay
    for (const [name, value] of Object.entries(layer)) {
      if (value !== undefined) merged[name] = value
    }
  }
  return merged as MergedOptions
}

// The statements send a `Date` as `toISOString` gives it, which PostgreSQL reads only for the years 1 to 9999: other years take a sign and six digits, or are year 0, which PostgreSQL lacks.
const minRunAt = Date.parse("0001-01-01T00:00:00.000Z")
const maxRunAt = Date.parse("9999-12-31T23:59:59.999Z")

/** Range of `runAt`, for error messages. */
const runAtRange = "a valid Date in the years 1 to 9999"

/** Whether a value is a `runAt` that the enqueue statement can send: a `Date` in the years 1 to 9999. */
function isRunAt(value: unknown): value is Date {
  return value instanceof Date && value.getTime() >= minRunAt && value.getTime() <= maxRunAt
}

/**
 * Checks merged options and turns them into the parameters of an enqueue of the core.
 * Fields that the key mode does not read, such as `maxWait` that a task in `debounce` mode leaves to an override of `keyMode: "replace"`, are ignored, since an override cannot remove a field.
 */
export function toEnqueueParams(task: string, payload: unknown, o: MergedOptions): EnqueueParams {
  const keyMode = o.keyMode ?? "replace"
  if (o.lease !== undefined || o.backoff !== undefined || o.scoped !== undefined) {
    throw new TypeError(
      "lease, backoff, and scoped are settings of defineTask, which an enqueue cannot override",
    )
  }
  if (o.delay != null && o.runAt != null)
    throw new TypeError("Pass either delay or runAt, not both")
  if (keyMode === "debounce" && (o.delay == null || o.maxWait == null || o.runAt != null)) {
    throw new TypeError('keyMode "debounce" takes delay and maxWait, and no runAt')
  }
  const maxWait = keyMode === "debounce" ? o.maxWait : null
  for (const [name, value] of [
    ["delay", o.delay],
    ["maxWait", maxWait],
  ] as const) {
    if (value != null && !isDelay(value)) {
      throw new RangeError(`${name} must be ${delayRange}, got ${String(value)}`)
    }
  }
  if (o.runAt != null && !isRunAt(o.runAt)) {
    throw new RangeError(`runAt must be ${runAtRange}, got ${String(o.runAt)}`)
  }
  if (o.maxAttempts != null) checkCount("maxAttempts", o.maxAttempts)
  return {
    task,
    payload,
    key: o.key,
    queue: o.queue,
    mode: keyMode,
    runAt: o.runAt,
    delay: o.delay,
    maxWait,
    maxAttempts: o.maxAttempts,
    resetAttempts: keyMode === "keep" ? undefined : o.resetAttempts,
  }
}
