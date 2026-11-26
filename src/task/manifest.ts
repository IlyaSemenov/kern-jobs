// Worker manifests collect task implementations and recurring schedules.
// An application can check handler coverage across all its manifests.

import { type Schedule, toWorkerSchedules } from "../schedule/schedule"

import { isTask, resolveFailure, type TaskBase, type TaskImplementation } from "./task"

/** Tasks a worker runs and schedules it evaluates. */
export interface Manifest {
  readonly tasks: readonly TaskImplementation<string, unknown>[]
  /** Recurring jobs this worker enqueues, including jobs handled by other workers in the same schema. */
  readonly schedules?: readonly Schedule[] | undefined
}

/**
 * Defines a worker manifest and rejects duplicate task names, among which the name of the failure task of an implementation counts, invalid failure implementations, invalid schedules, and duplicate schedule IDs.
 */
export function defineManifest(manifest: Manifest): Manifest {
  // The tasks are copied before their check, so that an addition to the list during the definition does not escape it; a copy turns the holes of a sparse array into `undefined`, which the check rejects.
  const list = manifest?.tasks
  const tasks = Array.isArray(list) ? [...list] : list
  toManifestTasks(tasks)
  const { schedules = [] } = manifest
  toWorkerSchedules(schedules)
  // Copy and freeze both lists so later additions cannot escape validation.
  return Object.freeze({
    tasks: Object.freeze(tasks),
    schedules: Object.freeze([...schedules]),
  })
}

/**
 * Defines a worker manifest with the tasks and schedules of the given manifests, in their order, such as of several modules of an application.
 * It checks each manifest as `defineManifest` does, and rejects a task name or schedule ID that two of them share.
 */
export function mergeManifests(...manifests: Manifest[]): Manifest {
  // Check each manifest first: flattening would accept a single task or schedule in place of its array and drop the holes of a sparse one.
  const defined = manifests.map(defineManifest)
  return defineManifest({
    tasks: defined.flatMap(manifest => manifest.tasks),
    schedules: defined.flatMap(manifest => manifest.schedules ?? []),
  })
}

/** Task that a worker of a manifest runs: of an implementation of the manifest, or the failure task of one. */
export interface ManifestTask {
  name: string
  /** Implementation whose handler runs the jobs of the task. */
  implementation: Pick<TaskImplementation<string, unknown>, "handler">
  /** Settings of the task, each read once and not checked. */
  settings: Pick<TaskBase<string, unknown>, "lease" | "backoff" | "scoped">
  /** Attempt budget of the failure jobs of the task, which its claims stamp, for an implementation with a failure task. */
  failureMaxAttempts?: number | undefined
}

/**
 * Returns the tasks that a worker of the implementations runs, in their order: the task of each implementation, followed by its failure task, if it has one.
 * It reads the task and the failure implementation of each implementation, the task of the failure implementation, and the settings of both tasks once, so that the manifest and the worker check and run the same values.
 * Throws `TypeError` for tasks that are not an array, for an element that is not an implementation, such as a task without `implement`, and for a failure task whose `scoped` differs from that of its task, whose handler is typed by it; the errors of `resolveFailure` for the failure implementation; and `Error` for a task name that several of the returned tasks have.
 */
export function toManifestTasks(
  tasks: readonly TaskImplementation<string, unknown>[],
): ManifestTask[] {
  if (!Array.isArray(tasks)) {
    throw new TypeError("manifest.tasks must be an array of task implementations")
  }
  const result: ManifestTask[] = []
  for (const implementation of tasks) {
    const task = implementation?.task
    const name = task?.name
    if (typeof implementation?.handler !== "function" || typeof name !== "string") {
      throw new TypeError(
        "A manifest takes implementations of tasks, which task.implement(handler) returns",
      )
    }
    const { failure } = implementation
    const settings = toSettings(task)
    const resolved = resolveFailure(name, failure)
    result.push({ name, implementation, settings, failureMaxAttempts: resolved?.maxAttempts })
    if (!failure || !resolved) continue
    const failureSettings = toSettings(resolved.task)
    if ((failureSettings.scoped === true) !== (settings.scoped === true)) {
      throw new TypeError(`scoped of failure task ${resolved.name} must be that of task ${name}`)
    }
    result.push({ name: resolved.name, implementation: failure, settings: failureSettings })
  }
  const names = new Set<string>()
  const duplicates = new Set<string>()
  for (const { name } of result) {
    if (names.has(name)) duplicates.add(name)
    else names.add(name)
  }
  if (duplicates.size > 0) {
    throw new Error(
      `A manifest takes one implementation of each task; duplicate tasks: ${[...duplicates].join(", ")}`,
    )
  }
  return result
}

/** Returns the settings of a task, each read once. */
function toSettings(task: TaskBase<string, unknown>): ManifestTask["settings"] {
  const { lease, backoff, scoped } = task
  return { lease, backoff, scoped }
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
    [manifests]
      .flat()
      .flatMap(manifest => toManifestTasks(manifest?.tasks))
      .map(task => task.name),
  )
  const names = Object.values(tasks)
    .filter(isTask)
    .map(task => task.name)
  return [...new Set(names)].filter(name => !implemented.has(name))
}
