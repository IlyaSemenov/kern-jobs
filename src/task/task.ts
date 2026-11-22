// Task definitions: the name of a task, the type of its payload, the options of its enqueues, and the settings of its workers, which producers and workers import from the same definition; and the implementations of tasks, which only workers import.
import { DEFAULT_MAX_ATTEMPTS } from "../sql/enqueue"
import { checkCount, checkStorableText, utf8Length } from "../sql/postgres"

import type { Backoff } from "./backoff"
import { FAILURE_TASK_SUFFIX, type JobFailure } from "./failure"
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
   * Implementation that runs the `onFailure` given to `implement`.
   * A wrapper that replaces `handler` must keep it, such as by `{ ...implementation, handler }`.
   */
  readonly failure?: FailureImplementation<Name, Payload, Scope> | undefined
  /**
   * Runs a job of the task: the job completes when the handler returns or its promise resolves, and fails when it throws or its promise rejects.
   */
  // A method, so that an implementation of any payload is a `TaskImplementation<string, unknown>`.
  handler(payload: Payload, context: HandlerContext<Scope>): unknown
}

/**
 * Implementation that runs the `onFailure` of a task implementation, available as its `failure`: a worker of that implementation runs it too, and `runJob` of `kern-jobs/testing` takes it as any implementation.
 * Its task, the failure task, has the name of the task with `:failure` after it, such as `orders.charge:failure`, under which the failure jobs show in the table, and the scope setting of the task.
 */
export interface FailureImplementation<
  Name extends string = string,
  Payload = unknown,
  Scope extends string | null = string | null,
> {
  readonly task: Task<`${Name}${typeof FAILURE_TASK_SUFFIX}`, JobFailure<Payload>, Scope>
  /** Runs a failure job of the task: the `onFailure` given to `implement`. */
  // A method, as the handler of a `TaskImplementation`.
  handler(failure: JobFailure<Payload>, context: HandlerContext<Scope>): unknown
}

/**
 * Handler that gets the `JobFailure` of a job of a task that failed for good, given to `implement` as `onFailure`.
 * It runs in a job of its own, the failure job: its context contains the failure job's metadata and abort signal.
 */
export type FailureHandler<Payload = unknown, Scope extends string | null = string | null> = (
  failure: JobFailure<Payload>,
  context: HandlerContext<Scope>,
) => unknown

/** Object form of `onFailure`: the handler with the settings of the retries of its failure jobs. */
export interface FailureOptions<Payload = unknown, Scope extends string | null = string | null> {
  handler: FailureHandler<Payload, Scope>
  /** Attempts of a failure job, after which it stays failed; 25 when omitted. */
  maxAttempts?: number | undefined
  /** Delay of the retry of a failure job after a failed attempt; `defaultBackoff` when omitted. */
  backoff?: Backoff | undefined
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
  /**
   * Implements the task with the handler of its jobs and the options of the implementation.
   * With `onFailure`, the implementation has the implementation of its failure task as `failure`.
   */
  implement(
    handler: (payload: Payload, context: HandlerContext<Scope>) => unknown,
    options: ImplementOptions<Payload, Scope> & {
      onFailure: FailureHandler<Payload, Scope> | FailureOptions<Payload, Scope>
    },
  ): TaskImplementation<Name, Payload, Scope> & {
    readonly failure: FailureImplementation<Name, Payload, Scope>
  }
  implement(
    handler: (payload: Payload, context: HandlerContext<Scope>) => unknown,
    options?: ImplementOptions<Payload, Scope>,
  ): TaskImplementation<Name, Payload, Scope>
}

/** Options of `task.implement`. */
export interface ImplementOptions<Payload = unknown, Scope extends string | null = string | null> {
  /**
   * Handles a failure for good of a job of this implementation, in a job of its own that is added in the transaction that records the failure and can run more than once.
   * Pass `{ handler, maxAttempts, backoff }` to set the retries of that job.
   */
  onFailure?: FailureHandler<Payload, Scope> | FailureOptions<Payload, Scope> | undefined
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
 * A name must not end with `:failure`, which names the failure task of a task.
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
  // Only `implement` gives such a name, so that no task of the application takes the failure jobs of another.
  if (isFailureTaskName(name)) {
    throw new RangeError(
      `The name of a task must not end with ${FAILURE_TASK_SUFFIX}, which names the failure task of a task, got ${name}`,
    )
  }
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

/**
 * Throws `TypeError` for a name of a task that is not storable text, and `RangeError` for one longer than 128 bytes in UTF-8.
 */
export function checkTaskName(name: unknown): asserts name is string {
  checkStorableText("The name of a task", name)
  if (utf8Length(name) > maxTaskNameBytes) {
    throw new RangeError(
      `The name of a task must be at most ${maxTaskNameBytes} bytes in UTF-8, got ${name}`,
    )
  }
}

/** Whether a value is a task, which `defineTask` returns. */
export function isTask(value: unknown): value is Task<string, unknown> {
  const task = value as Partial<Task<string, unknown>> | null | undefined
  return typeof task?.name === "string" && typeof task.implement === "function"
}

/** Returns the name of the failure task of the task of a name, under which its failure jobs are stored. */
function toFailureTaskName<Name extends string>(
  name: Name,
): `${Name}${typeof FAILURE_TASK_SUFFIX}` {
  return `${name}${FAILURE_TASK_SUFFIX}`
}

/** Whether a name is that of a failure task. */
function isFailureTaskName(name: string): boolean {
  return name.endsWith(FAILURE_TASK_SUFFIX)
}

/** Throws `TypeError` for the name of a failure task: the failure of a failure job stays failed and adds no failure job. */
function checkNoFailureOfFailure(name: string): void {
  if (isFailureTaskName(name)) {
    throw new TypeError(`Failure task ${name} takes no onFailure of its own`)
  }
}

/** Failure task of an implementation and the attempt budget of its failure jobs. */
interface ResolvedFailure {
  /** Name of the failure task, which the failure task was checked to have: the name under which the failure jobs are inserted. */
  name: string
  /** Failure task as read from the failure implementation, for the settings of its worker. */
  task: Task<string, unknown>
  /** `maxAttempts` of the options of the failure task, given to `onFailure`, or the default of an enqueue; the claims of the task stamp it, since the worker or the reaper that adds a failure job runs no code of the task. */
  maxAttempts: number
}

/**
 * Returns the failure task of the `failure` of an implementation of the task of a name, and the attempt budget of its failure jobs, or `undefined` without `failure`.
 * It reads the task of `failure` and the name of that task once, so that its caller checks and uses the same task under the checked name.
 * It throws `TypeError` for a `failure` of an implementation of a failure task, and for one that is not an implementation of the failure task of the task, and `RangeError` for a `maxAttempts` that is not a count.
 */
export function resolveFailure(name: string, failure: unknown): ResolvedFailure | undefined {
  if (failure === undefined) return undefined
  checkNoFailureOfFailure(name)
  const implementation = failure as Partial<FailureImplementation> | null
  const task = implementation?.task
  const failureName = toFailureTaskName(name)
  if (typeof implementation?.handler !== "function" || task?.name !== failureName) {
    throw new TypeError(
      `failure of the implementation of task ${name} must be the implementation of task ${failureName} that implement makes of onFailure`,
    )
  }
  // Only an absent budget takes the default, as in `implement`, so that a `null` is rejected rather than read as 25.
  const budget = task.options?.maxAttempts
  const maxAttempts = budget === undefined ? DEFAULT_MAX_ATTEMPTS : budget
  checkCount(`maxAttempts of failure task ${failureName}`, maxAttempts)
  return { name: failureName, task, maxAttempts }
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
    // The overloads of `implement` tell an implementation with a failure task from one without, which one function body cannot.
    implement: ((
      handler: TaskImplementation<Name, Payload>["handler"],
      options?: ImplementOptions<Payload>,
    ): TaskImplementation<Name, Payload> => {
      if (typeof handler !== "function")
        throw new TypeError("implement takes the handler of the task")
      if (options !== undefined && (typeof options !== "object" || options === null)) {
        throw new TypeError("The options of implement must be an object")
      }
      const onFailure = options?.onFailure
      if (onFailure === undefined) return { task, handler }
      checkNoFailureOfFailure(fields.name)
      return { task, handler, failure: createFailureImplementation(fields, onFailure) }
    }) as Task<Name, Payload>["implement"],
  }
  return task
}

/**
 * Creates the implementation of the failure task of a task from the `onFailure` given to `implement`.
 * The failure task has the name of the task with `:failure` after it and its scope setting, since a failure job has the scope of the failed job; it takes the `maxAttempts` and the backoff of `onFailure`, and neither the lease, the backoff, nor the options of an enqueue of the task.
 * It throws `TypeError` for an `onFailure` that is neither a function nor an object of a function `handler`, or for a backoff that is not a function, `RangeError` for a `maxAttempts` that is not a count, and the errors of `defineTask` for the name, such as `RangeError` for the name of a task too long to take the suffix.
 */
function createFailureImplementation<Name extends string, Payload>(
  fields: TaskFields<Name>,
  onFailure: FailureHandler<Payload> | FailureOptions<Payload>,
): FailureImplementation<Name, Payload> {
  const { handler, maxAttempts, backoff } =
    typeof onFailure === "function" ? { handler: onFailure } : (onFailure ?? {})
  if (typeof handler !== "function") {
    throw new TypeError(
      `onFailure of task ${fields.name} must be a handler of the failures of its jobs, or an object with it as handler`,
    )
  }
  if (backoff !== undefined && typeof backoff !== "function") {
    throw new TypeError(
      `backoff of onFailure of task ${fields.name} must be a function of the attempt`,
    )
  }
  const name = toFailureTaskName(fields.name)
  checkTaskName(name)
  if (maxAttempts !== undefined) {
    checkCount(`maxAttempts of onFailure of task ${fields.name}`, maxAttempts)
  }
  const options = maxAttempts === undefined ? {} : { maxAttempts }
  const task = createTask<typeof name, JobFailure<Payload>, object>(
    { name, lease: undefined, backoff, scoped: fields.scoped, options },
    {},
  )
  return { task, handler }
}
