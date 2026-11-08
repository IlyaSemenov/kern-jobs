// Task definitions: the name of a task, the type of its payload, the options of its enqueues, and the settings of its workers, which producers and workers import from the same definition; and the implementations of tasks, which only workers import.
import { utf8Length } from "../core/db"

import type { Backoff } from "./backoff"
import type { EnqueueOptions, StaticEnqueueOptions } from "./options"

/** Settings of a task, which an enqueue cannot override. */
export interface TaskSettings {
  /**
   * Lease of a job in seconds: a worker extends it while the handler runs, and once it expires, such as after the worker crashed, the job returns to pending, or fails on its last attempt; the lease of the worker when omitted.
   */
  lease?: number | undefined
  /** Delay of the retry after a failed attempt; `defaultBackoff` when omitted. */
  backoff?: Backoff | undefined
  /**
   * Whether each job of the task belongs to a scope, such as of a tenant: an enqueue takes the scope from `createJobs` and throws without one, and the handler gets it as `job.scope`.
   * The jobs of a task without it have no scope.
   */
  scoped?: boolean | undefined
}

/** Options of `defineTask`: the settings of the task and the options of each of its enqueues. */
export type TaskOptions = TaskSettings & StaticEnqueueOptions

/** Metadata of the job that a handler runs, whose scope is a `Scope`: a string for a task defined with `scoped: true`. */
export interface JobInfo<Scope extends string | null = string | null> {
  /** Id of the job, the same in each of its attempts. */
  id: string
  /** Scope of the job, given to `createJobs`, or `null` for a job without a scope. */
  scope: Scope
  /** Number of the attempt, from 1, which a retry of the failed job and an enqueue with `resetAttempts` start again; it is the last one when `attempt >= maxAttempts`. */
  attempt: number
  maxAttempts: number
  key: string | null
  queue: string | null
}

/** Context of a handler. */
export interface HandlerContext<Scope extends string | null = string | null> {
  /**
   * Aborted with a `HandlerAbort` of `kern-jobs/worker` when the worker can no longer keep the job, such as after its lease was lost, or when the worker stops.
   */
  signal: AbortSignal
  job: JobInfo<Scope>
}

/** Implementation of a task, which `task.implement` returns, for the manifest of a worker. */
export interface TaskImplementation<
  Name extends string = string,
  Payload = void,
  Scope extends string | null = string | null,
> {
  readonly task: Task<Name, Payload, Scope>
  /**
   * Runs a job of the task: the job completes when the handler returns or its promise resolves, and fails when it throws or its promise rejects.
   */
  // A method, so that an implementation of any payload is a `TaskImplementation<string, unknown>`.
  handler(payload: Payload, context: HandlerContext<Scope>): unknown
}

/**
 * Task that producers enqueue and workers run, defined with `defineTask`; `Scope` is the type of the scope of its jobs, `string` for a task defined with `scoped: true`.
 */
export interface Task<
  Name extends string = string,
  Payload = void,
  Scope extends string | null = string | null,
> {
  /** Name of the task, which identifies it in the database. */
  readonly name: Name
  /** Lease in seconds, given to `defineTask`. */
  readonly lease?: number | undefined
  /** Backoff, given to `defineTask`. */
  readonly backoff?: Backoff | undefined
  /** Whether each job of the task belongs to a scope, given to `defineTask`. */
  readonly scoped?: boolean | undefined
  /** Options of each enqueue, given to `defineTask`. */
  readonly options?: EnqueueOptions | undefined
  /** Returns the options of an enqueue of a payload, given with `withOptions`, which replace `options` field by field. */
  // Methods, so that a task of any payload is a `Task<string, unknown>`.
  optionsFor?(payload: Payload): EnqueueOptions
  /** Implements the task with the handler of its jobs. */
  implement(
    handler: (payload: Payload, context: HandlerContext<Scope>) => unknown,
  ): TaskImplementation<Name, Payload, Scope>
}

/** Task of `defineTask`, whose payload type and options computed from the payload can still be given. */
export interface TaskBuilder<
  Name extends string,
  Payload,
  Scope extends string | null = string | null,
> extends Task<Name, Payload, Scope> {
  /** Sets the payload type of the task. */
  withPayload<P>(): TaskBuilder<Name, P, Scope>
  /**
   * Sets a function that returns the options of each enqueue from its payload, such as a key, which replace the options given to `defineTask` field by field.
   */
  withOptions(options: (payload: Payload) => EnqueueOptions): Task<Name, Payload, Scope>
}

/**
 * Defines a task by its name, unique within the application, such as `email.send`, with its settings and the options of each of its enqueues.
 * The name stays in the jobs of the task in the database, so a worker of a renamed task does not run the pending jobs of its old name.
 */
export function defineTask<const Name extends string>(
  name: Name,
  options: TaskOptions & { scoped: true },
): TaskBuilder<Name, void, string>
export function defineTask<const Name extends string>(
  name: Name,
  options?: TaskOptions,
): TaskBuilder<Name, void>
export function defineTask(name: string, options: TaskOptions = {}): TaskBuilder<string, void> {
  const { lease, backoff, scoped, ...enqueueOptions } = options
  checkTaskName(name)
  if (lease !== undefined && !isLease(lease)) {
    throw new RangeError(`lease must be ${leaseRange}, got ${String(lease)}`)
  }
  if (backoff !== undefined && typeof backoff !== "function") {
    throw new TypeError("backoff must be a function of the attempt")
  }
  if (scoped !== undefined && typeof scoped !== "boolean") {
    throw new TypeError("scoped must be a boolean")
  }
  return createTaskBuilder({
    name,
    lease,
    backoff,
    scoped,
    options: enqueueOptions as EnqueueOptions,
  })
}

/** Longest name of a task in bytes of UTF-8, the limit of the column `task`. */
const maxTaskNameBytes = 128

/** Throws `TypeError` for a name of a task that is not a string, and `RangeError` for one longer than 128 bytes in UTF-8. */
function checkTaskName(name: unknown): asserts name is string {
  if (typeof name !== "string") throw new TypeError("The name of a task must be a string")
  if (utf8Length(name) > maxTaskNameBytes) {
    throw new RangeError(
      `The name of a task must be at most ${maxTaskNameBytes} bytes in UTF-8, got ${name}`,
    )
  }
}

/** Longest lease in seconds, about 31,700 years, a bound well within the intervals and timestamps of PostgreSQL. */
const maxLease = 1e12

/** Range of a lease, for error messages. */
export const leaseRange = "a number of seconds above 0 up to 1e12"

/** Whether a value is a lease of a task or of a worker, which a claim statement can add to the current time. */
export function isLease(value: unknown): value is number {
  return typeof value === "number" && value > 0 && value <= maxLease
}

type TaskFields<Name extends string> = Pick<
  Task<Name, never>,
  "name" | "lease" | "backoff" | "scoped" | "options"
>

function createTaskBuilder<Name extends string, Payload>(
  fields: TaskFields<Name>,
): TaskBuilder<Name, Payload> {
  return createTask(fields, {
    withPayload: () => createTaskBuilder(fields),
    withOptions: (optionsFor: (payload: Payload) => EnqueueOptions) => {
      if (typeof optionsFor !== "function") {
        throw new TypeError(
          "withOptions takes a function of the payload; pass static options to defineTask",
        )
      }
      return createTask({ ...fields, optionsFor }, {})
    },
  })
}

function createTask<Name extends string, Payload, Extra extends object>(
  fields: TaskFields<Name> & Pick<Task<Name, Payload>, "optionsFor">,
  extra: Extra,
): Task<Name, Payload> & Extra {
  const task: Task<Name, Payload> & Extra = {
    ...fields,
    ...extra,
    implement: handler => {
      if (typeof handler !== "function")
        throw new TypeError("implement takes the handler of the task")
      return { task, handler }
    },
  }
  return task
}
