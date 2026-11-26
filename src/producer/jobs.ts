// Jobs API of an application: enqueues of its tasks, and retries and deletes of its jobs, through the database client of the application.

import { checkStorableText, utf8Length } from "../limits"
import {
  type EnqueueItemsParams,
  type EnqueueJob,
  type EnqueueManyParams,
  type EnqueueResult,
  prepareEnqueueItems,
  prepareEnqueueMany,
} from "../sql/enqueue"
import { createJobTable, DEFAULT_SCHEMA_NAME } from "../sql/job-table"
import { delayRange, isDelay } from "../task/delay"
import {
  type BatchEnqueueOverrides,
  checkEnqueueOverrides,
  checkKey,
  type EnqueueOverrides,
  getTaskEnqueueParams,
  readOverrides,
  toStoredKey,
} from "../task/options"
import type { BatchTask, Task, TaskBase } from "../task/task"

import {
  deleteFailedJobs,
  deleteJob,
  deletePendingJobs,
  type FailedJobsFilter,
  type PendingJobsFilter,
  retryFailedJob,
  type RetryResult,
} from "./administration"
import { type JobsClient, toJobsTarget } from "./client"

/**
 * Value of the `scope` option of `createJobs` for code that manages the jobs of every scope, such as a script of an operator.
 * `retry` and `delete` act on the jobs of every scope, and so do `deletePending` and `deleteFailed` without a task or with a scoped task, while with another task they act on its unscoped jobs.
 * An enqueue of a scoped task rejects, since a job belongs to one scope.
 */
export const ALL_SCOPES: unique symbol = Symbol.for("kern-jobs.ALL_SCOPES")

/** Options of `createJobs`. */
export interface JobsOptions {
  /** PostgreSQL schema of the tables of kern-jobs, which `migrateJobs` created; `kern_jobs` by default. */
  schemaName?: string | undefined
  /**
   * Scope of the jobs of the tasks defined with `scoped: true`, such as the id of a tenant, or a function that returns it, which each enqueue of such a task and each delete of its pending or failed jobs calls, such as to read the tenant of the current request: a key merges and a queue serializes only the jobs of its scope.
   * An enqueue of another task adds an unscoped job, whatever this option.
   * `retry`, `delete`, and, without a task, `deletePending` and `deleteFailed` act only on the jobs of this scope, and call its function each time; without a scope, they act only on unscoped jobs, whose scope is empty.
   * With `ALL_SCOPES`, they and `deletePending` and `deleteFailed` of a scoped task act on the jobs of every scope.
   */
  scope?: string | (() => string) | typeof ALL_SCOPES | null | undefined
}

/** Pending jobs of a task that `deletePending` deletes: all of them, or those of a key. */
export interface DeletePendingFilter {
  task: TaskBase<string, unknown>
  /** Key of the pending jobs; `true` is the empty key, as in an enqueue. */
  key?: string | true
}

/**
 * Filter of the failed jobs that `deleteFailed` deletes, by task, by age, or both: with `olderThan`, it deletes only the jobs that failed more than that many seconds ago by the clock of the database.
 */
export type DeleteFailedFilter =
  | { task: TaskBase<string, unknown>; olderThan?: number }
  | { task?: TaskBase<string, unknown>; olderThan: number }

/** Jobs API of an application, created with `createJobs`. */
export interface Jobs {
  /**
   * Enqueues a job of a task with the options of the task and the overrides, in the transaction of the executor, so that workers see the job once the transaction commits.
   * Resolves to `{ id, created }` of the job that it inserted, or of the pending job of its key that it merged into.
   * It rejects with `TypeError` for a batch task, whose items `enqueueItems` enqueues.
   */
  enqueue<P>(
    task: Task<string, P>,
    ...args: NoInfer<
      undefined extends P
        ? [payload?: P, overrides?: EnqueueOverrides]
        : [payload: P, overrides?: EnqueueOverrides]
    >
  ): Promise<EnqueueResult>
  /**
   * Enqueues each payload of a task as `enqueue` does with the same overrides, in one statement: all of them or none, in the transaction of the executor.
   * Resolves to the result of each payload in their order; the new jobs get ids in that order, so a queue starts them in that order when they are due at the same time.
   * Without payloads, it resolves to `[]` without calling the client.
   * It takes each key of the task at most once, and rejects with `RangeError` for two payloads whose options give the same key, before it calls the client.
   * It locks the pending jobs of the keys in the order of the payloads, so two transactions that enqueue the same keys in different orders can deadlock, and PostgreSQL then aborts one of them.
   * It rejects with `TypeError` for a batch task, as `enqueue` does.
   */
  enqueueMany<P>(
    task: Task<string, P>,
    payloads: readonly NoInfer<P>[],
    overrides?: EnqueueOverrides,
  ): Promise<EnqueueResult[]>
  /**
   * Appends items to the pending job of a batch task of their key, in the transaction of the executor, with the options of the task.
   * The key comes from `overrides.key`, `withKey` of the task, or the options of `defineTask`.
   * The job takes items until a worker claims it or it has `maxItems`; the items that do not fit go to new jobs, full ones due at once and at most one partial one, which takes the items of later enqueues.
   * Resolves to `{ id, created }` of each job that took items, in the order of the items: the pending job of the key first, with `created: false`, if it took items, then the new jobs; `[]` for no items, without calling the client.
   * It rejects with `TypeError` for a task that is not a batch task, for items without a key or whose keys of `withKey` differ, for an override other than `key`, and for `keyMode: "keep"` of the task.
   */
  enqueueItems<I>(
    task: BatchTask<string, I>,
    items: readonly NoInfer<I>[],
    overrides?: BatchEnqueueOverrides,
  ): Promise<EnqueueResult[]>
  /**
   * Returns a failed job to pending with its payload and options and a full budget of attempts, to run as soon as a worker is free, and resolves to `{ retried: true }`.
   * When its key has a pending job that takes new enqueues, it changes neither job and resolves to `{ retried: false, pendingId }`; a job of a batch task returns with its own items whatever the pending jobs of its key.
   * Resolves to `null` when no failed job of the scope of `createJobs` has the id.
   * It can reject with the unique violation of the client, and change nothing, when an enqueue adds a new pending job of the key at the same time; the error aborts the transaction of the client.
   */
  retry(id: string): Promise<RetryResult | null>
  /**
   * Deletes a pending or failed job, and resolves to `false` when no such job of the scope of `createJobs` has the id, such as one that a worker runs; a pending job of a key takes with it the events that the enqueues of the key merged into it.
   */
  delete(id: string): Promise<boolean>
  /**
   * Deletes the pending jobs of every task of the scope of `createJobs`, or of every scope with `ALL_SCOPES`, each with the events that the enqueues of its key merged into it, and resolves to their count.
   * When `createJobs` has no scope, it deletes only unscoped jobs.
   * Jobs that run or failed stay, and so does a pending job that becomes visible after the statement of the delete takes its snapshot, such as from an enqueue or a failed attempt of a running job.
   */
  deletePending(): Promise<number>
  /**
   * Deletes the pending jobs of a task, or those of its key, each with the events that the enqueues of its key merged into it, and resolves to their count.
   * The jobs of a task are those of the scope of `createJobs` for a scoped task, or of every scope with `ALL_SCOPES`, and those without a scope for another task.
   * Jobs that run or failed stay, and so does a pending job that becomes visible after the statement of the delete takes its snapshot, such as from an enqueue or a failed attempt of a running job.
   */
  deletePending(filter: DeletePendingFilter): Promise<number>
  /**
   * Deletes the failed jobs of every task of the scope of `createJobs`, or of every scope with `ALL_SCOPES`, and resolves to their count.
   * When `createJobs` has no scope, it deletes only unscoped jobs.
   * Pending and running jobs stay, and so does a failed job that a retry returns to pending while the delete runs.
   */
  deleteFailed(): Promise<number>
  /**
   * Deletes the failed jobs that match the filter, by task, by age, or both, and resolves to their count; with `olderThan`, only the jobs that failed more than that many seconds ago by the clock of the database.
   * The jobs of a task are those of the scope of `createJobs` for a scoped task, or of every scope with `ALL_SCOPES`, and those without a scope for another task; without a task, those of the scope of `createJobs`, as without a filter.
   * Pending and running jobs stay, and so does a failed job that a retry returns to pending while the delete runs.
   */
  deleteFailed(filter: DeleteFailedFilter): Promise<number>
}

/**
 * Creates the jobs API of an application, which enqueues tasks, and retries and deletes jobs, through the database client of the application.
 * `client` is one of:
 * - an executor, an object with `query(text, values)` that resolves to `{ rows }`, such as a `pg.Pool`;
 * - a client of postgres.js, `sql` or a transaction of it;
 * - a function that returns one of them, called for each statement of the jobs API so that it runs in the transaction that the application holds at the call;
 * - a `Database` of kern-db or a `Db` of kern-orm, whose calls of the jobs API join its current transaction.
 * It throws `TypeError` for a client of another kind, and a call of the jobs API rejects with it for a function that returns one.
 * It throws `TypeError` for a scope other than a string, a function, `ALL_SCOPES`, `null`, or `undefined`, or with U+0000 or a lone surrogate, and `RangeError` for an empty one or one longer than 128 bytes in UTF-8.
 * `enqueue`, `enqueueMany`, `enqueueItems`, `deletePending`, and `deleteFailed` of a scoped task reject with `TypeError` without a scope, and the enqueues of a scoped task also with `ALL_SCOPES`.
 * Each call that calls the function of the scope, the enqueues, `deletePending`, and `deleteFailed` of a scoped task, `deletePending` and `deleteFailed` without a task, `retry`, and `delete`, rejects with the errors of `createJobs` for a scope that it returns.
 * A retry and a delete reject with `TypeError` for an id that is not a string, and `RangeError` for one that is not a decimal integer from 1 to 2^63 − 1, before they call the client.
 * `deletePending` rejects with `TypeError` for an argument that is not an object of a task and, optionally, its key, for a task name that is not a string, for a key that is neither `true` nor a string, and for a name or a key string that has U+0000 or a lone surrogate, before it calls the client.
 * `deleteFailed` rejects with `TypeError` for an argument that is not an object of a task, `olderThan`, or both, and for a task name that is not storable text, and with `RangeError` for `olderThan` that is not a number of seconds from 0 to 1e12, before it calls the client.
 */
export function createJobs(client: JobsClient, options: JobsOptions = {}): Jobs {
  const target = toJobsTarget(client)
  const { schemaName = DEFAULT_SCHEMA_NAME, scope } = options
  const table = createJobTable(schemaName)
  return createJobsApi(
    {
      // Each payload is serialized before the client factory is called, which an enqueue without a statement never calls.
      enqueueMany: params => prepareEnqueueMany(table, params)(target),
      enqueueItems: params => prepareEnqueueItems(table, params)(target),
      retry: params => retryFailedJob(target(), table, params),
      delete: params => deleteJob(target(), table, params),
      deletePending: filter => deletePendingJobs(target(), table, filter),
      deleteFailed: filter => deleteFailedJobs(target(), table, filter),
    },
    scope,
  )
}

/**
 * Store of jobs under the jobs API: the SQL of the core for `createJobs`, or the memory of the test double of `kern-jobs/testing`.
 * It takes parameters that the jobs API checked and whose scope it resolved: a string, the empty one for unscoped jobs, or `null` for every scope.
 */
export interface JobStore {
  /** Enqueues jobs of one task and scope, at least one, whose keys are distinct, all or none, and resolves to the result of each in their order. */
  enqueueMany(params: Omit<EnqueueManyParams, "now"> & { scope: string }): Promise<EnqueueResult[]>
  /** Appends the items of an enqueue of a batch task, the array of its payload, and resolves to the result of each job that took items, in their order. */
  enqueueItems(
    params: Omit<EnqueueItemsParams, "now"> & { scope: string },
  ): Promise<EnqueueResult[]>
  retry(params: { id: string; scope: string | null }): Promise<RetryResult | null>
  delete(params: { id: string; scope: string | null }): Promise<boolean>
  deletePending(filter: PendingJobsFilter & { scope: string | null }): Promise<number>
  deleteFailed(filter: FailedJobsFilter & { scope: string | null }): Promise<number>
}

/**
 * Creates the jobs API over a store: it merges and checks the options of an enqueue, checks the arguments of administration, and resolves the scope of each call, before it calls the store.
 * It throws the errors of `createJobs` for a scope of another kind.
 */
export function createJobsApi(store: JobStore, scope: JobsOptions["scope"]): Jobs {
  if (typeof scope === "string") checkScope(scope)
  else if (scope != null && typeof scope !== "function" && scope !== ALL_SCOPES) {
    throw new TypeError("scope must be a string, a function that returns one, or ALL_SCOPES")
  }
  return {
    async enqueue(
      task: Task<string, unknown>,
      payload?: unknown,
      overrides?: EnqueueOverrides,
    ): Promise<EnqueueResult> {
      checkNotBatchTask(task)
      overrides = readOverrides(task, overrides, "The overrides of an enqueue")
      const params = getTaskEnqueueParams(task, payload, overrides)
      const [result] = await store.enqueueMany({
        task: params.task,
        scope: getEnqueueScope(task, scope),
        jobs: [params],
      })
      return result!
    },
    async enqueueMany(
      task: Task<string, unknown>,
      payloads: readonly unknown[],
      overrides?: EnqueueOverrides,
    ): Promise<EnqueueResult[]> {
      checkNotBatchTask(task)
      if (!Array.isArray(payloads)) throw new TypeError("enqueueMany takes an array of payloads")
      overrides = readOverrides(task, overrides, "The overrides of an enqueue")
      // `Array.from` visits the holes of a sparse array too, as a loop of enqueues does, which `map` skips.
      const jobs = Array.from(payloads, payload => getTaskEnqueueParams(task, payload, overrides))
      if (jobs.length === 0) {
        // No payload merges the options, so the checks that an enqueue makes run on the name and the overrides alone, and so does the check of the scope, without the client.
        checkStorableText("The name of a task", task.name)
        checkEnqueueOverrides(overrides)
        getEnqueueScope(task, scope)
        return []
      }
      checkDistinctKeys(jobs)
      return store.enqueueMany({ task: jobs[0]!.task, scope: getEnqueueScope(task, scope), jobs })
    },
    async enqueueItems(
      task: BatchTask<string, unknown>,
      items: readonly unknown[],
      overrides?: BatchEnqueueOverrides,
    ): Promise<EnqueueResult[]> {
      // The kind is checked first, so that a task of another kind runs no function of its options.
      const { maxItems } = task
      if (maxItems === undefined) {
        throw new TypeError(`Task ${task.name} is not a batch task: enqueue it with enqueue`)
      }
      overrides = readOverrides(task, overrides, "The overrides of an enqueue")
      const params = getTaskEnqueueParams(task, items, overrides as EnqueueOverrides)
      return store.enqueueItems({ ...params, maxItems, scope: getEnqueueScope(task, scope) })
    },
    async retry(id: string): Promise<RetryResult | null> {
      checkJobId(id)
      return store.retry({ id, scope: getJobsScope(scope) })
    },
    async delete(id: string): Promise<boolean> {
      checkJobId(id)
      return store.delete({ id, scope: getJobsScope(scope) })
    },
    async deletePending(...args: [] | [filter: DeletePendingFilter]): Promise<number> {
      // Only a call without an argument deletes the pending jobs of every task, so that an argument that names no task, such as `undefined`, does not.
      if (args.length === 0) return store.deletePending({ scope: getJobsScope(scope) })
      const { task, key } = parseDeletePendingFilter(args[0])
      return store.deletePending({ scope: getTaskScope(task, scope), task: task.name, key })
    },
    async deleteFailed(...args: [] | [filter: DeleteFailedFilter]): Promise<number> {
      // Only a call without an argument deletes the failed jobs of every task at any age, so that an argument that names neither, such as `undefined`, does not.
      const { task, olderThan } =
        args.length === 0
          ? { task: undefined, olderThan: undefined }
          : parseDeleteFailedFilter(args[0])
      const jobScope = task ? getTaskScope(task, scope) : getJobsScope(scope)
      return store.deleteFailed({ scope: jobScope, task: task?.name, olderThan })
    },
  }
}

/**
 * Returns the scope of the jobs that an enqueue of a task adds, as `getTaskScope` gives it, and throws `TypeError` for `ALL_SCOPES` with a scoped task, since a job belongs to one scope.
 */
export function getEnqueueScope(
  task: TaskBase<string, unknown>,
  scope: JobsOptions["scope"],
): string {
  const taskScope = getTaskScope(task, scope)
  if (taskScope === null) {
    throw new TypeError(
      `Task ${task.name} is scoped: pass createJobs the scope of its jobs rather than ALL_SCOPES`,
    )
  }
  return taskScope
}

/** Throws `TypeError` for a batch task, whose items only `enqueueItems` enqueues. */
function checkNotBatchTask(task: TaskBase<string, unknown>): void {
  if (task.maxItems !== undefined) {
    throw new TypeError(`Task ${task.name} is a batch task: enqueue its items with enqueueItems`)
  }
}

/**
 * Throws `RangeError` for two jobs of one `enqueueMany` with the same key, which one statement cannot merge both into the pending job of the key.
 * Its jobs share their task and scope, so their stored keys tell them apart.
 */
function checkDistinctKeys(jobs: readonly EnqueueJob[]): void {
  const keys = new Set<string>()
  for (const { key } of jobs) {
    if (key == null) continue
    if (keys.has(key)) {
      throw new RangeError(`enqueueMany takes each key once, got ${JSON.stringify(key)} twice`)
    }
    keys.add(key)
  }
}

/** Largest id of a job, the largest PostgreSQL `bigint`. */
const maxJobId = 2n ** 63n - 1n

/**
 * Throws `TypeError` for an id of a job that is not a string, and `RangeError` for one that is not a decimal integer from 1 to 2^63 − 1, the range of the ids of the job table; `name` names the id in the error.
 */
export function checkJobId(id: unknown, name = "id"): asserts id is string {
  if (typeof id !== "string") throw new TypeError(`${name} must be a string, got ${String(id)}`)
  if (!/^[1-9][0-9]{0,18}$/.test(id) || BigInt(id) > maxJobId) {
    throw new RangeError(`${name} must be a decimal integer from 1 to ${maxJobId}, got "${id}"`)
  }
}

/** Task of a filter of a delete: its name and `scoped`, each read once. */
interface FilterTask {
  name: string
  scoped: boolean | undefined
}

/**
 * Throws `error` for a filter of a delete that is not an object or has an own property other than `fields`, such as a task passed as the filter or a misspelled field, since the delete would ignore what it does not know and widen to more jobs.
 * Each parser then reads the fields from the filter itself in its own order, each once, so that the statement gets the values that were checked.
 */
function checkFilterFields(
  filter: unknown,
  fields: readonly string[],
  error: TypeError,
): asserts filter is Record<string, unknown> {
  // Own keys of every kind, so that a symbol or a non-enumerable property is not ignored either.
  if (
    typeof filter !== "object" ||
    filter === null ||
    Reflect.ownKeys(filter).some(field => typeof field !== "string" || !fields.includes(field))
  ) {
    throw error
  }
}

/**
 * Returns the name and `scoped` of the task of a filter of a delete, each read once, and throws `error` for a task that is not an object, and `TypeError` for a name that is not storable text.
 */
function toFilterTask(task: unknown, error: TypeError): FilterTask {
  if (typeof task !== "object" || task === null) throw error
  const { name, scoped } = task as TaskBase<string, unknown>
  checkStorableText("The name of a task", name)
  return { name, scoped }
}

/**
 * Returns the task of a filter of `deletePending` and its key, read in this order.
 * Throws `TypeError` for a filter that is not an object of a task and, optionally, its key, and for a task name that is not storable text or a key that is neither `true` nor storable text.
 */
function parseDeletePendingFilter(filter: unknown): {
  task: FilterTask
  key: string | undefined
} {
  const error = new TypeError("deletePending takes a filter of a task and, optionally, its key")
  checkFilterFields(filter, ["task", "key"], error)
  const task = toFilterTask(filter.task, error)
  // A key that is given, also by a getter of a class, must be one, so that a missing value does not widen the delete to every key.
  if (!("key" in filter)) return { task, key: undefined }
  const { key } = filter
  checkKey(key)
  return { task, key: toStoredKey(key) }
}

/**
 * Returns the task of a filter of `deleteFailed` and its `olderThan`, read in this order.
 * Throws `TypeError` for a filter that is not an object of a task, `olderThan`, or both, and for a task name that is not storable text, and `RangeError` for `olderThan` that is not a number of seconds from 0 to 1e12.
 */
function parseDeleteFailedFilter(filter: unknown): {
  task: FilterTask | undefined
  olderThan: number | undefined
} {
  const error = new TypeError("deleteFailed takes a filter of a task, olderThan, or both")
  checkFilterFields(filter, ["task", "olderThan"], error)
  // A field that is given, also by a getter of a class, must be valid, so that a missing value does not widen the delete; its presence is checked when the fields before it are read, as for `key` of `deletePending`.
  const task = "task" in filter ? toFilterTask(filter.task, error) : undefined
  if (!("olderThan" in filter)) {
    // A filter names what it deletes, so that `{}` does not widen to every failed job as a call without an argument does.
    if (task === undefined) throw error
    return { task, olderThan: undefined }
  }
  const { olderThan } = filter
  // The bound of a delay keeps the age within the intervals and timestamps of PostgreSQL.
  if (!isDelay(olderThan)) {
    throw new RangeError(`olderThan must be ${delayRange}, got ${String(olderThan)}`)
  }
  return { task, olderThan }
}

/**
 * Returns the scope of the jobs of a task that the jobs API reaches: for a scoped task, that of `getJobsScope`, and throws `TypeError` when `createJobs` has no scope; for another task, the empty scope, without calling the function of the scope.
 */
export function getTaskScope(
  task: { name: string; scoped?: boolean | undefined },
  scope: JobsOptions["scope"],
): string | null {
  if (task.scoped !== true) return ""
  if (scope == null)
    throw new TypeError(`Task ${task.name} is scoped: pass its scope to createJobs`)
  return getJobsScope(scope)
}

/**
 * Returns the scope of the jobs that the jobs API reaches: the scope of `createJobs`, or the result of its function, which it checks; the empty scope, of unscoped jobs, when `createJobs` has none; and `null`, every scope, for `ALL_SCOPES`.
 */
function getJobsScope(scope: JobsOptions["scope"]): string | null {
  if (scope === ALL_SCOPES) return null
  // Without a scope, only unscoped jobs, so that a jobs API that misses its scope never reaches the jobs of a tenant.
  if (scope == null) return ""
  if (typeof scope === "string") return scope
  const value = scope()
  checkScope(value)
  return value
}

/** Longest scope in bytes of UTF-8, the limit of the column `scope`. */
const maxScopeBytes = 128

/**
 * Throws `TypeError` for a scope that is not storable text, and `RangeError` for an empty one, which the table stores for jobs without a scope, or one longer than 128 bytes in UTF-8.
 */
export function checkScope(scope: unknown): asserts scope is string {
  checkStorableText("scope", scope)
  if (scope === "" || utf8Length(scope) > maxScopeBytes) {
    throw new RangeError(`scope must be 1 to ${maxScopeBytes} bytes in UTF-8, got "${scope}"`)
  }
}
