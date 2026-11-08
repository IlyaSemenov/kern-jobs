// Jobs API of an application: enqueues of its tasks, and retries and deletes of its jobs, through the database client of the application.
import { deleteJob, retryFailedJob, type RetryResult } from "../core/administration"
import { utf8Length } from "../core/db"
import { enqueue, type EnqueueResult } from "../core/enqueue"
import { createJobTable, DEFAULT_SCHEMA } from "../core/schema"
import { type JobsClient, toJobsTarget } from "../core/statement"
import { type EnqueueOverrides, mergeOptions, toEnqueueParams } from "../task/options"
import type { Task } from "../task/task"

/** Options of `createJobs`. */
export interface JobsOptions {
  /** PostgreSQL schema of the tables of kern-jobs, which `migrateJobs` created; `kern_jobs` by default. */
  schemaName?: string | undefined
  /**
   * Scope of the jobs of the tasks defined with `scoped: true`, such as the id of a tenant, or a function that returns it, which each enqueue of such a task calls, such as to read the tenant of the current request: a key merges and a queue serializes only the jobs of its scope.
   * The jobs of other tasks have no scope, whatever this option.
   * A retry and a delete act only on the jobs of this scope, and call its function each time; without a scope, they act on the jobs of every scope.
   */
  scope?: string | (() => string) | null | undefined
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
}

/**
 * Creates the jobs API of an application, which enqueues tasks, and retries and deletes jobs, through the database client of the application.
 * `client` is one of:
 * - an executor, an object with `query(text, values)` that resolves to `{ rows }`, such as a `pg.Pool`;
 * - a client of postgres.js, `sql` or a transaction of it;
 * - a function that returns one of them, called at each call of the jobs API so that it runs in the transaction that the application holds at the call;
 * - a `Database` of kern-db or a `Db` of kern-orm, whose calls of the jobs API join its current transaction.
 * It throws `TypeError` for a client of another kind, and a call of the jobs API rejects with it for a function that returns one.
 * It throws `TypeError` for a scope other than a string, a function, `null`, or `undefined`, and `RangeError` for an empty one or one longer than 128 bytes in UTF-8.
 * An enqueue of a scoped task rejects with `TypeError` without a scope, and with the errors of `createJobs` for a scope that the function returns, as a retry and a delete do.
 * A retry and a delete reject with `TypeError` for an id that is not a string, and `RangeError` for one that is not a decimal integer from 1 to 2^63 − 1, before they call the client.
 */
export function createJobs(client: JobsClient, options: JobsOptions = {}): Jobs {
  const target = toJobsTarget(client)
  const { schemaName = DEFAULT_SCHEMA, scope } = options
  const table = createJobTable(schemaName)
  if (typeof scope === "string") checkScope(scope)
  else if (scope != null && typeof scope !== "function") {
    throw new TypeError("scope must be a string or a function that returns one")
  }
  return {
    async enqueue(
      task: Task<string, unknown>,
      payload?: unknown,
      overrides?: EnqueueOverrides,
    ): Promise<EnqueueResult> {
      const params = toEnqueueParams(
        task.name,
        payload,
        mergeOptions(task.options, task.optionsFor?.(payload), overrides),
      )
      if (task.scoped === true) params.scope = toScope(task.name, scope)
      return enqueue(target(), table, params)
    },
    async retry(id: string): Promise<RetryResult | null> {
      checkJobId(id)
      const jobScope = resolveScope(scope)
      return retryFailedJob(target(), table, { id, scope: jobScope })
    },
    async delete(id: string): Promise<boolean> {
      checkJobId(id)
      const jobScope = resolveScope(scope)
      return deleteJob(target(), table, { id, scope: jobScope })
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

/** Returns the scope of an enqueue of a scoped task, and throws `TypeError` when `createJobs` has none. */
function toScope(task: string, scope: JobsOptions["scope"]): string {
  const value = resolveScope(scope)
  if (value === null) throw new TypeError(`Task ${task} is scoped: pass its scope to createJobs`)
  return value
}

/** Returns the scope of `createJobs`, or the result of its function, which it checks, or `null` without a scope. */
function resolveScope(scope: JobsOptions["scope"]): string | null {
  if (scope == null) return null
  if (typeof scope === "string") return scope
  const value = scope()
  checkScope(value)
  return value
}

/** Longest scope in bytes of UTF-8, the limit of the column `scope`. */
const maxScopeBytes = 128

/**
 * Throws `TypeError` for a scope that is not a string, and `RangeError` for an empty one, which the table stores for jobs without a scope, or one longer than 128 bytes in UTF-8.
 */
function checkScope(scope: unknown): asserts scope is string {
  if (typeof scope !== "string") throw new TypeError(`scope must be a string, got ${String(scope)}`)
  if (scope === "" || utf8Length(scope) > maxScopeBytes) {
    throw new RangeError(`scope must be 1 to ${maxScopeBytes} bytes in UTF-8, got "${scope}"`)
  }
}
