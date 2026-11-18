// Worker manifests collect task implementations and recurring schedules.
// An application can check handler coverage across all its manifests.
import { type Schedule, toWorkerSchedules } from "../schedule/schedule"

import { isTask, type TaskImplementation } from "./task"

/** Tasks a worker runs and schedules it evaluates. */
export interface Manifest {
  readonly tasks: readonly TaskImplementation<string, unknown>[]
  /** Recurring jobs this worker enqueues, including jobs handled by other workers in the same schema. */
  readonly schedules?: readonly Schedule[] | undefined
}

/**
 * Defines a worker manifest and rejects duplicate task names, invalid schedules, and duplicate schedule IDs.
 */
export function defineManifest(manifest: Manifest): Manifest {
  const tasks = [...toTaskMap(manifest?.tasks).values()]
  const { schedules = [] } = manifest
  toWorkerSchedules(schedules)
  // Copy and freeze both lists so later additions cannot escape validation.
  return Object.freeze({
    tasks: Object.freeze(tasks),
    schedules: Object.freeze([...schedules]),
  })
}

/**
 * Returns the implementations by task name, in their order.
 * Throws `TypeError` for tasks that are not an array or for an element that is not an implementation, such as a task without `implement`, and `Error` for task names of several implementations.
 */
export function toTaskMap(
  tasks: readonly TaskImplementation<string, unknown>[],
): Map<string, TaskImplementation<string, unknown>> {
  const map = new Map<string, TaskImplementation<string, unknown>>()
  const duplicates = new Set<string>()
  if (!Array.isArray(tasks)) {
    throw new TypeError("manifest.tasks must be an array of task implementations")
  }
  for (const implementation of tasks) {
    if (
      typeof implementation?.handler !== "function" ||
      typeof implementation.task?.name !== "string"
    ) {
      throw new TypeError(
        "A manifest takes implementations of tasks, which task.implement(handler) returns",
      )
    }
    const { name } = implementation.task
    if (map.has(name)) duplicates.add(name)
    else map.set(name, implementation)
  }
  if (duplicates.size > 0) {
    throw new Error(
      `A manifest takes one implementation of each task; duplicate tasks: ${[...duplicates].join(", ")}`,
    )
  }
  return map
}

/**
 * Returns the names of the tasks that none of the manifests implements, for a test of an application: a job of such a task stays pending, since no worker runs it.
 * It takes the tasks as the values of an object, such as the exports of the module that defines them, and skips the values that are not tasks.
 */
export function findUnimplementedTasks(
  tasks: Readonly<Record<string, unknown>>,
  manifests: Manifest | readonly Manifest[],
): string[] {
  const implemented = new Set(
    [manifests].flat().flatMap(manifest => [...toTaskMap(manifest?.tasks).keys()]),
  )
  const names = Object.values(tasks)
    .filter(isTask)
    .map(task => task.name)
  return [...new Set(names)].filter(name => !implemented.has(name))
}
