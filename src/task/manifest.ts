// Manifest of a worker: the implementations of the tasks that it runs, one per task name, and the check that every task of an application has one.
import type { Task, TaskImplementation } from "./task"

/** Implementations of the tasks that a worker runs, defined with `defineManifest`. */
export interface Manifest {
  readonly tasks: readonly TaskImplementation<string, unknown>[]
}

/**
 * Defines the manifest of a worker from the implementations of its tasks.
 * It throws when two implementations have the same task name, as soon as the module that defines the manifest runs.
 */
export function defineManifest(manifest: Manifest): Manifest {
  const tasks = [...toTaskMap(manifest?.tasks).values()]
  // Frozen, so that an implementation added later does not escape the check.
  return Object.freeze({ tasks: Object.freeze(tasks) })
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

/** Whether a value is a task, which `defineTask` returns. */
function isTask(value: unknown): value is Task<string, unknown> {
  const task = value as Partial<Task<string, unknown>> | null | undefined
  return typeof task?.name === "string" && typeof task.implement === "function"
}
