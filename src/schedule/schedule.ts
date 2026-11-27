import { checkScope, isStorableText, storableTextRule, utf8Length } from "../limits"
import {
  type BatchEnqueueOverrides,
  checkEnqueueOverrides,
  type EnqueueOverrides,
  getTaskEnqueueParams,
  readOverrides,
} from "../task/options"
import { type BatchTask, checkTaskName, isTask, type Task, type TaskBase } from "../task/task"

import { compileCron, type CronDefinition } from "./cron"

/**
 * Scopes of the jobs of a schedule of a scoped task, such as the ids of tenants: an array of them, or a function that returns one or a promise of one, such as from a registry of tenants.
 * A worker calls the function without arguments at each check that finds an occurrence due, so several workers may call it at once for one occurrence, of which the jobs of one list are enqueued; the worker waits for it also while it stops, so it must bound its waits, such as for a registry, by a timeout.
 * A scope listed more than once counts once, and no scopes enqueue nothing.
 */
export type ScheduleScopes =
  | readonly string[]
  | (() => readonly string[] | Promise<readonly string[]>)

/**
 * Options of `defineSchedule`, including the static payload required by the task, the overrides `O` of its enqueue, and the scopes by the scope type `S` of the task.
 * Without `S`, they are the options of a task that is not scoped, which have no scopes.
 */
export type ScheduleOptions<P, O = EnqueueOverrides, S extends string | null = null> = {
  /** Schedule ID, which identifies the schedule among the workers of one schema and defaults to the task name. */
  id?: string | undefined
  /** Structured UTC calendar or five-field UTC cron string. */
  cron: string | CronDefinition
  /** Overrides applied to the task options at each scheduled enqueue. */
  enqueue?: O | undefined
  /** Set to false to skip occurrences during an intentional pause. */
  enabled?: boolean | undefined
  /** Enqueue once if a missed occurrence falls within this many seconds of the check. */
  catchUp?: { within: number } | undefined
} & (undefined extends P ? { payload?: P } : { payload: P }) &
  ([S] extends [string]
    ? {
        /** Scopes in each of which an occurrence enqueues the task. */
        scopes: ScheduleScopes
      }
    : [S] extends [null]
      ? {
          /** Given for a scoped task only. */
          scopes?: undefined
        }
      : {
          /** Scopes in each of which an occurrence enqueues the task, which a scoped task requires and another task rejects. */
          scopes?: ScheduleScopes | undefined
        })

/** Schedule returned by `defineSchedule` for the `schedules` of a worker manifest. */
export interface Schedule<P = unknown> {
  readonly id: string
  readonly task: TaskBase<string, P>
  readonly cron: string | CronDefinition
  readonly payload: P
  readonly enqueue?: EnqueueOverrides | undefined
  readonly scopes?: ScheduleScopes | undefined
  readonly enabled: boolean
  readonly catchUp?: { readonly within: number } | undefined
}

/** Schedule of a worker, checked and with its calendar compiled for evaluation. */
export interface WorkerSchedule {
  readonly id: string
  readonly task: TaskBase<string, unknown>
  readonly payload: unknown
  readonly enqueue: EnqueueOverrides | undefined
  /**
   * Scopes of the jobs of a scoped task: those of an array, as `toScheduleScopes` returns them, or the function that returns the scopes of a check, whose result `toScheduleScopes` takes; `undefined` for a task that is not scoped, whose job has no scope.
   */
  readonly scopes: readonly string[] | (() => unknown) | undefined
  readonly enabled: boolean
  /** Catch-up window in seconds, or zero when catch-up is disabled. */
  readonly within: number
  /** Five sorted sets of the calendar, which `hasOccurrence` takes. */
  readonly calendar: number[][]
}

/**
 * Defines a UTC schedule of a task with its payload and enqueue overrides, and, for a scoped task, the scopes in each of which an occurrence enqueues it.
 * The payload of a batch task is its array of items, and a batch task must not be scoped.
 */
// The scope type has a default, that of a task whose type does not tell whether it is scoped, so that a call that gives the payload type alone still compiles.
export function defineSchedule<P, S extends string | null = string | null>(
  task: Task<string, P, S>,
  options: NoInfer<ScheduleOptions<P, EnqueueOverrides, S>>,
): Schedule<P>
export function defineSchedule<I>(
  task: BatchTask<string, I>,
  options: NoInfer<ScheduleOptions<I[], BatchEnqueueOverrides>>,
): Schedule<I[]>
export function defineSchedule<P>(
  task: TaskBase<string, P>,
  options: ScheduleOptions<P, EnqueueOverrides, string | null>,
): Schedule<P> {
  const schedule: Schedule<P> = {
    id: options.id ?? task.name,
    task,
    cron: options.cron,
    payload: options.payload as P,
    enqueue: options.enqueue,
    scopes: options.scopes,
    enabled: options.enabled ?? true,
    catchUp: options.catchUp,
  }
  toWorkerSchedule(schedule)
  return Object.freeze(schedule)
}

/**
 * Returns the schedule of a worker: checks the definition of a schedule and compiles its calendar.
 * A worker checks again what `defineSchedule` checked, since any object of the shape of a schedule passes the types.
 */
export function toWorkerSchedule(schedule: Schedule): WorkerSchedule {
  if (!schedule || typeof schedule !== "object") {
    throw new TypeError("A manifest takes schedules defined with defineSchedule")
  }
  const { id, task, payload, scopes, enabled, catchUp } = schedule
  if (typeof id !== "string" || !id || !isStorableText(id)) {
    throw new TypeError(
      `Schedule ID must be a string that is not empty and has ${storableTextRule}`,
    )
  }
  if (utf8Length(id) > 128) {
    throw new RangeError("Schedule ID must be at most 128 bytes in UTF-8")
  }
  if (!isTask(task) || (task.optionsFor !== undefined && typeof task.optionsFor !== "function")) {
    throw new TypeError(`Schedule ${id} must have a task defined with defineTask`)
  }
  checkTaskName(task.name)
  const { scoped } = task
  if (scoped !== undefined && typeof scoped !== "boolean") {
    throw new TypeError(`scoped of task ${task.name} must be a boolean`)
  }
  // The items of a scoped batch task would take a lock of their key in each scope until the transaction of the check ends, which the lock table of the server bounds.
  if (scoped && task.maxItems !== undefined) {
    throw new TypeError(`Cannot schedule scoped batch task ${task.name}`)
  }
  // A job of a scoped task belongs to a scope, and a job of another task to none, so neither kind of task leaves the choice to the schedule.
  if (scoped && scopes === undefined) {
    throw new TypeError(`Schedule ${id} of scoped task ${task.name} must give its scopes`)
  }
  if (!scoped && scopes !== undefined) {
    throw new TypeError(`Schedule ${id} of task ${task.name}, which is not scoped, takes no scopes`)
  }
  // The worker enqueues the copy of an array at every occurrence, and checks what a function returns at each of its calls.
  const workerScopes =
    scopes === undefined || typeof scopes === "function" ? scopes : toScheduleScopes(id, scopes)
  // The worker reuses these checked override values for every occurrence.
  const enqueue = readOverrides(task, schedule.enqueue, `enqueue of schedule ${id}`)
  if (typeof enabled !== "boolean") {
    throw new TypeError(`enabled of schedule ${id} must be a boolean`)
  }
  if (catchUp !== undefined && (!Number.isSafeInteger(catchUp?.within) || catchUp.within <= 0)) {
    throw new RangeError(
      `catchUp.within of schedule ${id} must be a positive whole number of seconds`,
    )
  }
  const calendar = compileCron(schedule.cron)
  if (task.optionsFor) {
    // The function of `withOptions` runs only at an enqueue, which checks the merged options; a task without it is checked here, calling `withKey` for the static payload.
    checkEnqueueOverrides(enqueue)
  } else {
    getTaskEnqueueParams(task, payload, enqueue)
  }
  return {
    id,
    task,
    payload,
    enqueue,
    scopes: workerScopes,
    enabled,
    within: catchUp?.within ?? 0,
    calendar,
  }
}

/**
 * Returns the scopes in which a check of a schedule enqueues, from the array of the schedule or from what its function returned: each scope once, sorted by UTF-16 code units, so that checks whose scopes overlap enqueue them, and lock their keys, in one order.
 * It throws `TypeError` for a value that is not an array, and the errors of `checkScope` for an element of it, so that an invalid scope rejects the whole list.
 */
export function toScheduleScopes(id: string, scopes: unknown): string[] {
  if (!Array.isArray(scopes)) {
    throw new TypeError(
      `scopes of schedule ${id} must be an array of scopes or a function that returns one`,
    )
  }
  const unique = new Set<string>()
  // `for…of` visits the holes of a sparse array, which the check rejects.
  for (const scope of scopes) {
    checkScope(scope)
    unique.add(scope)
  }
  return [...unique].sort()
}

/** Returns the schedules of a worker from those of a manifest, none when omitted, rejecting an invalid list and a reused schedule ID. */
export function toWorkerSchedules(
  schedules: readonly Schedule[] | undefined = [],
): WorkerSchedule[] {
  if (!Array.isArray(schedules)) {
    throw new TypeError("manifest.schedules must be an array of schedules")
  }
  const ids = new Set<string>()
  const result: WorkerSchedule[] = []
  // `for…of` visits the holes of a sparse array, which `map` would skip.
  for (const schedule of schedules) {
    const workerSchedule = toWorkerSchedule(schedule)
    if (ids.has(workerSchedule.id)) {
      throw new Error(`Duplicate schedule ID in a manifest: ${workerSchedule.id}`)
    }
    ids.add(workerSchedule.id)
    result.push(workerSchedule)
  }
  return result
}
