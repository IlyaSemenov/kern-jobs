// Jobs API of an application: enqueues of its tasks, and retries and deletes of its jobs, through the database client of the application.
import {
  deleteJob,
  deletePendingJobs,
  retryFailedJob,
  type RetryResult,
} from "../core/administration"
import { checkStorableText, utf8Length } from "../core/db"
import { enqueue, type EnqueueResult } from "../core/enqueue"
import { createJobTable, DEFAULT_SCHEMA } from "../core/schema"
import { type JobsClient, toJobsTarget } from "../core/statement"
import { type EnqueueOverrides, getTaskEnqueueParams } from "../task/options"
import type { Task } from "../task/task"

/**
 * Value of the `scope` option of `createJobs` for code that manages the jobs of every scope, such as a script of an operator: `retry`, `delete`, and `deletePending` without a filter or of a scoped task act on the jobs of every scope, while `deletePending` of another task acts on its unscoped jobs, and an enqueue of a scoped task rejects, since a job belongs to one scope.
 */
export const ALL_SCOPES: unique symbol = Symbol.for("kern-jobs.ALL_SCOPES")

/** Options of `createJobs`. */
export interface JobsOptions {
  /** PostgreSQL schema of the tables of kern-jobs, which `migrateJobs` created; `kern_jobs` by default. */
  schemaName?: string | undefined
  /**
   * Scope of the jobs of the tasks defined with `scoped: true`, such as the id of a tenant, or a function that returns it, which each enqueue of such a task and each delete of its pending jobs calls, such as to read the tenant of the current request: a key merges and a queue serializes only the jobs of its scope.
   * An enqueue of another task adds an unscoped job, whatever this option.
   * `retry`, `delete`, and `deletePending` without a filter act only on the jobs of this scope, and call its function each time; without a scope, they act only on unscoped jobs, whose scope is empty.
   * With `ALL_SCOPES`, they and `deletePending` of a scoped task act on the jobs of every scope.
   */
  scope?: string | (() => string) | typeof ALL_SCOPES | null | undefined
}

/** Pending jobs of a task that `deletePending` deletes: all of them, or those of a key, at most one in each scope. */
export interface DeletePendingFilter {
  task: Task<string, unknown>
  key?: string
}

/** Jobs API of an application, created with `createJobs`. */
export interface Jobs {
  /**
   * Enqueues a job of a task with the options of the task and the overrides, in the transaction of the executor, so that workers see the job once the transaction commits.
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
   * Returns a failed job to pending with its payload and options and a full budget of attempts, to run as soon as a worker is free, and resolves to `{ retried: true }`.
   * When a pending job of its key exists, it changes neither job and resolves to `{ retried: false, pendingId }`.
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
   * Deletes the pending jobs of a task, or those of its key, at most one in each scope, each with the events that the enqueues of its key merged into it, and resolves to their count.
   * The jobs of a task are those of the scope of `createJobs` for a scoped task, or of every scope with `ALL_SCOPES`, and those without a scope for another task.
   * Jobs that run or failed stay, and so does a pending job that becomes visible after the statement of the delete takes its snapshot, such as from an enqueue or a failed attempt of a running job.
   */
  deletePending(filter: DeletePendingFilter): Promise<number>
}

/**
 * Creates the jobs API of an application, which enqueues tasks, and retries and deletes jobs, through the database client of the application.
 * `client` is one of:
 * - an executor, an object with `query(text, values)` that resolves to `{ rows }`, such as a `pg.Pool`;
 * - a client of postgres.js, `sql` or a transaction of it;
 * - a function that returns one of them, called at each call of the jobs API so that it runs in the transaction that the application holds at the call;
 * - a `Database` of kern-db or a `Db` of kern-orm, whose calls of the jobs API join its current transaction.
 * It throws `TypeError` for a client of another kind, and a call of the jobs API rejects with it for a function that returns one.
 * It throws `TypeError` for a scope other than a string, a function, `ALL_SCOPES`, `null`, or `undefined`, or with U+0000 or a lone surrogate, and `RangeError` for an empty one or one longer than 128 bytes in UTF-8.
 * `enqueue` and `deletePending` of a scoped task reject with `TypeError` without a scope, and `enqueue` of a scoped task also with `ALL_SCOPES`.
 * Each call that calls the function of the scope, `enqueue` and `deletePending` of a scoped task, `deletePending` without a filter, `retry`, and `delete`, rejects with the errors of `createJobs` for a scope that it returns.
 * A retry and a delete reject with `TypeError` for an id that is not a string, and `RangeError` for one that is not a decimal integer from 1 to 2^63 − 1, before they call the client.
 * `deletePending` rejects with `TypeError` for an argument that is not an object of a task and, optionally, its key, and for a task name or a key that is not a string, or has U+0000 or a lone surrogate, before it calls the client.
 */
export function createJobs(client: JobsClient, options: JobsOptions = {}): Jobs {
  const target = toJobsTarget(client)
  const { schemaName = DEFAULT_SCHEMA, scope } = options
  const table = createJobTable(schemaName)
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
      const params = getTaskEnqueueParams(task, payload, overrides)
      const taskScope = getTaskScope(task, scope)
      if (taskScope === null) {
        throw new TypeError(
          `Task ${task.name} is scoped: pass createJobs the scope of its jobs rather than ALL_SCOPES`,
        )
      }
      params.scope = taskScope
      return enqueue(target(), table, params)
    },
    async retry(id: string): Promise<RetryResult | null> {
      checkJobId(id)
      const jobScope = getJobsScope(scope)
      return retryFailedJob(target(), table, { id, scope: jobScope })
    },
    async delete(id: string): Promise<boolean> {
      checkJobId(id)
      const jobScope = getJobsScope(scope)
      return deleteJob(target(), table, { id, scope: jobScope })
    },
    async deletePending(...args: [] | [filter: DeletePendingFilter]): Promise<number> {
      // Only a call without an argument deletes the pending jobs of every task, so that an argument that names no task, such as `undefined`, does not.
      if (args.length === 0) {
        const jobScope = getJobsScope(scope)
        return deletePendingJobs(target(), table, { scope: jobScope })
      }
      const { name, scoped, key } = parseDeletePendingFilter(args[0])
      const jobScope = getTaskScope({ name, scoped }, scope)
      return deletePendingJobs(target(), table, { scope: jobScope, task: name, key })
    },
  }
}

/** Largest id of a job, the largest PostgreSQL `bigint`. */
const maxJobId = 2n ** 63n - 1n

/**
 * Throws `TypeError` for an id of a job that is not a string, and `RangeError` for one that is not a decimal integer from 1 to 2^63 − 1, the range of the ids of the job table.
 */
function checkJobId(id: unknown): asserts id is string {
  if (typeof id !== "string") throw new TypeError(`id must be a string, got ${String(id)}`)
  if (!/^[1-9][0-9]{0,18}$/.test(id) || BigInt(id) > maxJobId) {
    throw new RangeError(`id must be a decimal integer from 1 to ${maxJobId}, got "${id}"`)
  }
}

/** Fields of a filter of `deletePending`. */
const deletePendingFields: ReadonlySet<string> = new Set(["task", "key"])

/**
 * Returns the name and `scoped` of the task of a filter of `deletePending`, and its key, each read once, so that the statement gets the values that were checked.
 * Throws `TypeError` for a filter that is not an object of a task and, optionally, its key, such as a task passed as the filter, a promise of a filter, or a misspelled field, since the delete would ignore what it does not know and widen to more jobs, and for a task name or a key that is not storable text.
 */
function parseDeletePendingFilter(filter: unknown): {
  name: string
  scoped: boolean | undefined
  key: string | undefined
} {
  const error = new TypeError("deletePending takes a filter of a task and, optionally, its key")
  // Own keys of every kind, so that a symbol or a non-enumerable property is not ignored either.
  if (
    typeof filter !== "object" ||
    filter === null ||
    Reflect.ownKeys(filter).some(
      field => typeof field !== "string" || !deletePendingFields.has(field),
    )
  ) {
    throw error
  }
  const { task } = filter as { task?: unknown }
  if (typeof task !== "object" || task === null) throw error
  const { name, scoped } = task as Task<string, unknown>
  checkStorableText("The name of a task", name)
  // A key that is given, also by a getter of a class, must be one, so that a missing value does not widen the delete to every key.
  if (!("key" in filter)) return { name, scoped, key: undefined }
  const { key } = filter as { key?: unknown }
  checkStorableText("key", key)
  return { name, scoped, key }
}

/**
 * Returns the scope of the jobs of a task that the jobs API reaches: for a scoped task, that of `getJobsScope`, and throws `TypeError` when `createJobs` has no scope; for another task, the empty scope, without calling the function of the scope.
 */
function getTaskScope(
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
function checkScope(scope: unknown): asserts scope is string {
  checkStorableText("scope", scope)
  if (scope === "" || utf8Length(scope) > maxScopeBytes) {
    throw new RangeError(`scope must be 1 to ${maxScopeBytes} bytes in UTF-8, got "${scope}"`)
  }
}
