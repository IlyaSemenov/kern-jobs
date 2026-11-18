import { isStorableText, storableTextRule, utf8Length } from "../core/db"
import { checkEnqueueOverrides, type EnqueueOverrides, getTaskEnqueueParams } from "../task/options"
import { checkTaskName, isTask, type Task } from "../task/task"

import { compileCron, type CronDefinition } from "./cron"

/** Options of `defineSchedule`, including the static payload required by the task. */
export type ScheduleOptions<P> = {
  /** Schedule ID, which identifies the schedule among the workers of one schema and defaults to the task name. */
  id?: string | undefined
  /** Structured UTC calendar or five-field UTC cron string. */
  cron: string | CronDefinition
  /** Overrides applied to the task options at each scheduled enqueue. */
  enqueue?: EnqueueOverrides | undefined
  /** Set to false to skip occurrences during an intentional pause. */
  enabled?: boolean | undefined
  /** Enqueue one job if a missed occurrence falls within this many seconds of the check. */
  catchUp?: { within: number } | undefined
} & (undefined extends P ? { payload?: P } : { payload: P })

/** Schedule returned by `defineSchedule` for the `schedules` of a worker manifest. */
export interface Schedule<P = unknown> {
  readonly id: string
  readonly task: Task<string, P>
  readonly cron: string | CronDefinition
  readonly payload: P
  readonly enqueue?: EnqueueOverrides | undefined
  readonly enabled: boolean
  readonly catchUp?: { readonly within: number } | undefined
}

/** Schedule of a worker, checked and with its calendar compiled for evaluation. */
export interface WorkerSchedule {
  readonly id: string
  readonly task: Task<string, unknown>
  readonly payload: unknown
  readonly enqueue: EnqueueOverrides | undefined
  readonly enabled: boolean
  /** Catch-up window in seconds, or zero when catch-up is disabled. */
  readonly within: number
  /** Five sorted sets of the calendar, which `hasOccurrence` takes. */
  readonly calendar: number[][]
}

/** Defines a UTC schedule for an unscoped task with its payload and enqueue overrides. */
export function defineSchedule<P>(
  task: Task<string, P>,
  options: NoInfer<ScheduleOptions<P>>,
): Schedule<P> {
  const schedule: Schedule<P> = {
    id: options.id ?? task.name,
    task,
    cron: options.cron,
    payload: options.payload as P,
    enqueue: options.enqueue,
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
  const { id, task, payload, enqueue, enabled, catchUp } = schedule
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
  if (task.scoped) {
    throw new TypeError(`Cannot schedule scoped task ${task.name}`)
  }
  if (
    enqueue !== undefined &&
    (!enqueue || typeof enqueue !== "object" || Array.isArray(enqueue))
  ) {
    throw new TypeError(`enqueue of schedule ${id} must be an object of enqueue options`)
  }
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
    // The function of `withOptions` runs only at an enqueue.
    checkEnqueueOverrides(enqueue)
  } else {
    getTaskEnqueueParams(task, payload, enqueue)
  }
  return { id, task, payload, enqueue, enabled, within: catchUp?.within ?? 0, calendar }
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
