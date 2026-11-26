// Options of an enqueue: what a task definition and an enqueue give, their merge, and the parameters of the core.

import { checkCount, checkStorableText, isStorableText, storableTextRule } from "../limits"
import type { EnqueueParams } from "../sql/enqueue"

import { delayRange, isDelay } from "./delay"
import type { TaskBase } from "./task"

/** Options of the key, the queue, and the attempts of a job, which the options of `defineTask` and the overrides of `jobs.enqueue` take. */
export interface CommonEnqueueOptions {
  /**
   * Key of the pending job within its task, such as `product:42`; `true` is the empty key, `""`, and `null` enqueues the job without a key.
   */
  key?: string | true | null | undefined
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
export type StaticEnqueueOptions = CommonEnqueueOptions & TimeOptions & KeyModeOptions

/** Options of an enqueue of a payload, which the function of `withOptions` returns: those of `defineTask` other than the key, which `withKey` gives. */
export type EnqueueOptions = StaticEnqueueOptions &
  NoSettings & {
    /** Given by `withKey`, `defineTask`, or the enqueue. */
    key?: undefined
  }

/**
 * Options of one enqueue that replace those of its task: a field given as `undefined` keeps the field of the task, and giving `delay` or `runAt` drops the other.
 */
export type EnqueueOverrides = CommonEnqueueOptions & TimeOptions & KeyModeOverrides & NoSettings

/**
 * Overrides of an enqueue of a batch task, which `jobs.enqueueItems` takes: only its key, since the other options apply to the whole batch, which holds the items of other enqueues too, and come from `defineTask`.
 */
export type BatchEnqueueOverrides = {
  /** Key of the pending job that the items append to, within its task, such as `site:42`; `true` is the empty key, `""`. */
  key?: string | true | undefined
} & {
  // The other overrides are rejected in an object of any type too, such as one shared with the enqueues of a task that is not a batch task.
  [Name in Exclude<keyof EnqueueOverrides, "key">]?: undefined
}

/**
 * Resolves the enqueue parameters shared by producers and schedules, with overrides applied last; for a batch task, the payload is its array of items.
 * A key of the overrides replaces that of `withKey`, which is then not called.
 */
export function getTaskEnqueueParams<P>(
  task: TaskBase<string, P>,
  payload: P,
  overrides?: EnqueueOverrides,
): TaskEnqueueParams {
  const { maxItems } = task
  const { keyFor } = task as KeyFunction
  const ownKey = keyFor === undefined || overrides?.key !== undefined ? undefined : keyFor
  if (maxItems === undefined) {
    const key = ownKey && { key: getPayloadKey(task.name, ownKey(payload)) }
    const options = task.optionsFor?.(payload)
    if (options?.key !== undefined) {
      throw new TypeError(`withOptions of task ${task.name} cannot return a key; use withKey`)
    }
    return toEnqueueParams(task.name, payload, mergeOptions(task.options, key, options, overrides))
  }
  checkBatchOverrides(task.name, overrides)
  const key =
    ownKey && Array.isArray(payload) ? { key: getItemsKey(task.name, payload, ownKey) } : undefined
  return toEnqueueParams(task.name, payload, mergeOptions(task.options, key, overrides), maxItems)
}

/** Function of `withKey` of a task of either kind, which takes a payload or an item. */
interface KeyFunction {
  keyFor?: ((unit: unknown) => unknown) | undefined
}

/**
 * Checks the overrides of an enqueue of a batch task: throws `TypeError` for an override other than `key`, since the other options apply to the whole batch.
 */
function checkBatchOverrides(
  task: string,
  overrides: EnqueueOverrides | BatchEnqueueOverrides | undefined,
): void {
  if (overrides == null) return
  for (const [name, value] of Object.entries(overrides)) {
    if (value !== undefined && name !== "key") {
      throw new TypeError(
        `An enqueue of batch task ${task} takes only a key, got ${name}; its other options come from defineTask`,
      )
    }
  }
}

/** Returns the key that `withKey` of a task that is not a batch task returned for a payload, `null` for none, and throws `TypeError` for another value that is not storable text. */
function getPayloadKey(task: string, key: unknown): string | null {
  if (key === null) return null
  checkComputedKey(task, key)
  return key
}

/** Checks a key that `withKey` of a task returned: throws `TypeError` unless it is storable text. */
function checkComputedKey(task: string, key: unknown): asserts key is string {
  checkStorableText(`The key of withKey of task ${task}`, key)
}

/**
 * Returns the key of the items of an enqueue of a batch task by its function of `withKey`, called once for each item, or `undefined` for no items.
 * It throws `TypeError` for a key that is not storable text, and for items whose keys differ, since one enqueue appends to one key.
 */
function getItemsKey(
  task: string,
  items: readonly unknown[],
  keyFor: (item: unknown) => unknown,
): string | undefined {
  let first: string | undefined
  for (const item of items) {
    const key: unknown = keyFor(item)
    checkComputedKey(task, key)
    if (first === undefined) first = key
    else if (key !== first) {
      throw new TypeError(
        `The items of an enqueue of batch task ${task} must have one key, got ${JSON.stringify(first)} and ${JSON.stringify(key)}`,
      )
    }
  }
  return first
}

/** Parameters of an enqueue of a task: with `maxItems` for a batch task, whose payload is its array of items. */
export type TaskEnqueueParams = EnqueueParams & { maxItems?: number | undefined }

/** Options after merging one or more layers. */
interface MergedOptions {
  key?: string | true | null
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
  ...layers: (StaticEnqueueOptions | EnqueueOptions | EnqueueOverrides | undefined)[]
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
 * Checks the overrides of an enqueue without the layers before them, such as those of a schedule whose task computes its options at each enqueue.
 * It rejects only errors that no earlier layer can correct; the enqueue checks the merged options.
 */
export function checkEnqueueOverrides(overrides: EnqueueOverrides | undefined): void {
  checkOptions(mergeOptions(overrides), true)
}

/**
 * Checks merged options, or with `partial`, the last layer alone, whose omitted fields earlier layers may still give.
 * Fields that the key mode does not read, such as `maxWait` that a task in `debounce` mode leaves to an override of `keyMode: "replace"`, are ignored, since an override cannot remove a field.
 */
function checkOptions(o: MergedOptions, partial: boolean): void {
  if (o.lease !== undefined || o.backoff !== undefined || o.scoped !== undefined) {
    throw new TypeError(
      "lease, backoff, and scoped are settings of defineTask, which an enqueue cannot override",
    )
  }
  if (o.delay != null && o.runAt != null)
    throw new TypeError("Pass either delay or runAt, not both")
  const debounce = o.keyMode === "debounce"
  // A field that the last layer gives, also as `null` past the types, is final; even its `runAt: null` clears an inherited delay, so a debounce layer must then give its own.
  const missing = partial
    ? o.delay === null || o.maxWait === null || (o.delay === undefined && o.runAt === null)
    : o.delay == null || o.maxWait == null
  if (debounce && (missing || o.runAt != null)) {
    throw new TypeError('keyMode "debounce" takes delay and maxWait, and no runAt')
  }
  for (const [name, value] of [
    ["delay", o.delay],
    ["maxWait", debounce ? o.maxWait : null],
  ] as const) {
    if (value != null && !isDelay(value)) {
      throw new RangeError(`${name} must be ${delayRange}, got ${String(value)}`)
    }
  }
  if (o.runAt != null && !isRunAt(o.runAt)) {
    throw new RangeError(`runAt must be ${runAtRange}, got ${String(o.runAt)}`)
  }
  if (o.maxAttempts != null) checkCount("maxAttempts", o.maxAttempts)
  if (o.key != null) checkKey(o.key)
  if (o.queue != null) checkStorableText("queue", o.queue)
}

/** Checks a key of the API: throws `TypeError` unless it is `true` or storable text. */
export function checkKey(key: unknown): asserts key is string | true {
  if (key === true || (typeof key === "string" && isStorableText(key))) return
  const got = typeof key === "string" ? JSON.stringify(key) : String(key)
  throw new TypeError(`key must be true or a string that has ${storableTextRule}, got ${got}`)
}

/** Returns the key that a job stores for a key of the API, in which `true` is the empty key. */
export function toStoredKey(key: string | true): string {
  return key === true ? "" : key
}

/**
 * Checks the name of the task and the merged options, and turns them into the parameters of an enqueue of the core, of a batch with `maxItems`.
 * It checks the name again since a task defined by an older version of kern-jobs may carry a name that `defineTask` now rejects.
 */
export function toEnqueueParams(
  task: string,
  payload: unknown,
  o: MergedOptions,
  maxItems?: number,
): TaskEnqueueParams {
  checkStorableText("The name of a task", task)
  checkOptions(o, false)
  if (maxItems !== undefined) checkBatch(task, payload, o, maxItems)
  const keyMode = o.keyMode ?? "replace"
  const maxWait = keyMode === "debounce" ? o.maxWait : null
  return {
    task,
    payload,
    key: o.key == null ? o.key : toStoredKey(o.key),
    queue: o.queue,
    // The mode stays absent when no layer gave it, like the other options; the core enqueues it as `replace`.
    mode: o.keyMode,
    runAt: o.runAt,
    delay: o.delay,
    maxWait,
    maxAttempts: o.maxAttempts,
    resetAttempts: keyMode === "keep" ? undefined : o.resetAttempts,
    maxItems,
  }
}

/**
 * Checks an enqueue of a batch task: throws `RangeError` for a `maxItems` that is not a count, and `TypeError` for a payload that is not an array, merged options without a key for items, which identifies the batch that they append to, and `keyMode: "keep"`, which `withBatchPayload` rejects too but a task of the shape of a batch task can give.
 */
function checkBatch(task: string, payload: unknown, o: MergedOptions, maxItems: number): void {
  checkCount(`maxItems of task ${task}`, maxItems)
  if (!Array.isArray(payload)) {
    throw new TypeError(`The items of batch task ${task} must be an array`)
  }
  // An enqueue without items appends to no key.
  if (o.key == null && payload.length > 0) {
    throw new TypeError(`Batch task ${task} requires a key, such as key: true or withKey`)
  }
  checkBatchKeyMode(task, o.keyMode)
}

/** Throws `TypeError` for `keyMode: "keep"` of a batch task, which would drop the items of the enqueues after the first. */
export function checkBatchKeyMode(task: string, keyMode: string | undefined): void {
  if (keyMode === "keep") {
    throw new TypeError(`keyMode "keep" does not apply to batch task ${task}`)
  }
}
