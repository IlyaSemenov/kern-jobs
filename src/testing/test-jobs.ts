// Test double of the jobs API: it records the enqueues of the code under test in memory, without a database, and checks them as `createJobs` does.

import {
  createJobsApi,
  getEnqueueScope,
  getTaskScope,
  type Jobs,
  type JobsOptions,
} from "../producer/jobs"
import type { EnqueueJob, EnqueueResult } from "../sql/enqueue"
import { encodePayload } from "../sql/job"
import type { FailureImplementation, TaskBase, TaskImplementation } from "../task/task"

import { type RunFailureArgs, type RunFailureOptions, runFailureWithScope } from "./run-failure"
import { type RunJobArgs, type RunJobOptions, runJobWithScope } from "./run-job"

/** Options of `createTestJobs`. */
export interface TestJobsOptions {
  /** Scope of the jobs of the tasks defined with `scoped: true`, as the `scope` of `createJobs`. */
  scope?: JobsOptions["scope"]
}

/**
 * Options of a recorded enqueue: those of `defineTask`, of `withKey` and `withOptions`, and the overrides, merged as an enqueue merges them.
 * An option that no layer gave is absent, and so are `maxWait` outside `keyMode: "debounce"` and `resetAttempts` with `keyMode: "keep"`, which the enqueue ignores.
 * `key: true` is recorded as the empty key, `""`, which the enqueue stores.
 */
export interface TestEnqueueOptions {
  key?: string | null
  queue?: string | null
  maxAttempts?: number
  delay?: number
  runAt?: Date
  keyMode?: "replace" | "keep" | "debounce"
  maxWait?: number
  resetAttempts?: boolean
}

/** Enqueue that `createTestJobs` recorded. */
export interface TestJobCall<P = unknown> {
  /** Name of the task. */
  task: string
  /** Scope of the job, or `null` for a job without a scope, as a handler gets it in `job.scope`. */
  scope: string | null
  /** Payload as the code passed it to `enqueue`, or the array of items of `enqueueItems`. */
  payload: P
  options: TestEnqueueOptions
}

/** Enqueues that `capture` of `createTestJobs` recorded, and the result of its action. */
export interface TestJobQueries<R = unknown> {
  /** Enqueues in the order that this capture recorded them. */
  readonly calls: readonly TestJobCall[]
  /** Value that the action returned or resolved to. */
  readonly result: R
  /**
   * Returns the enqueues of a task, as `deletePending({ task })` selects its pending jobs: of a task defined with `scoped: true`, those of its name in the scope of `createTestJobs` at the call, or in every scope with `ALL_SCOPES`, such as those of an unscoped definition of the name; of another task, those of its name without a scope.
   * It throws `TypeError` for a scoped task when `createTestJobs` has no scope.
   */
  callsFor<P>(task: TaskBase<string, P>): TestJobCall<P>[]
  /** Returns the payloads of the enqueues that `callsFor` returns. */
  payloadsFor<P>(task: TaskBase<string, P>): P[]
}

/** Jobs API of tests, which `createTestJobs` returns. */
export interface TestJobs extends Jobs {
  /** Enqueues since `createTestJobs` or the latest `clear`, in the order that the code made them. */
  readonly calls: readonly TestJobCall[]
  /** Forgets the recorded enqueues and the pending jobs; a running `capture` keeps the enqueues that it recorded. */
  clear(): void
  /**
   * Runs an action, and resolves to the recorded enqueues and the awaited result of the action, or rejects with its error.
   * A capture records every enqueue through this jobs API while it runs, also those of concurrent code and of nested and concurrent captures: until an action that returns a value other than a promise returns, or, for a promise, until `capture` resumes after it settles.
   * Finish the setup of fixtures before the capture, and await it before the next step, so that their enqueues stay out.
   */
  capture<R>(action: () => R): Promise<TestJobQueries<Awaited<R>>>
  /**
   * Runs the handler of a task implementation as `runJob` does, and gives its job a scope as an enqueue through this jobs API does.
   * A task defined with `scoped: true` gets the scope of `createTestJobs` at the call, and another task gets none.
   * `options.job.scope` replaces that scope, and then the scope function of `createTestJobs` is not called.
   * It rejects with the errors of `runJob`.
   * Without `options.job.scope`, a scoped task also rejects with the errors of `enqueue` for the scope, such as `TypeError` when `createTestJobs` has no scope or has `ALL_SCOPES`.
   */
  runJob<P>(
    implementation: TaskImplementation<string, P>,
    ...args: NoInfer<RunJobArgs<P, string | null>>
  ): Promise<unknown>
  /**
   * Runs the `onFailure` of a task implementation as `runFailure` does, and gives the failed job the scope that `runJob` of this jobs API gives a job: of a scoped task, the scope of `createTestJobs` at the call.
   * `options.job.scope` replaces that scope, and then the scope function of `createTestJobs` is not called.
   * It rejects with the errors of `runFailure`, and without `options.job.scope`, for a scoped task, with the errors of `enqueue` for the scope.
   */
  runFailure<P>(
    implementation: TaskImplementation<string, P> & {
      readonly failure: FailureImplementation<string, P>
    },
    ...args: NoInfer<RunFailureArgs<P, string | null>>
  ): Promise<unknown>
}

/** Pending job of the memory of `createTestJobs`. */
interface PendingJob {
  id: string
  task: string
  /** Scope as the table stores it: empty for a job without a scope. */
  scope: string
  key: string | null
  /** Items of a job of a batch task, and its capacity; absent for another job. */
  batch?: { items: number; maxItems: number }
  /** Whether the job takes no more enqueues of its key: a job of a batch task that is full or of a capacity that a later enqueue changed. */
  sealed: boolean
}

/**
 * Creates the jobs API of tests: it records each enqueue rather than writing it to a database, and keeps its jobs pending, since no worker runs them.
 * It merges and checks the options of an enqueue, serializes its payload, resolves its scope, and checks the arguments of administration as `createJobs` does, and rejects with the same errors before it records anything, also with the error of `JSON.stringify` for a payload, such as a bigint; it does not check the constraints of the table, such as the length of a key.
 * `enqueueMany` records an enqueue of each payload in their order, and none of them when it rejects.
 * An enqueue with the key of a pending job of its task and scope that takes new enqueues resolves to the id of that job and `created: false`, in every key mode; it rejects with an `Error` of the message and the `code` of the error of the database when it would merge a batch job and a job that is not a batch.
 * `enqueueItems` records one enqueue of its array, also of an empty one, and resolves to the jobs that its items fill by their count: the pending job of its key until it has `maxItems`, then new ones; a pending job of another `maxItems` takes no more items.
 * `delete` and `deletePending` delete the pending jobs that they reach by the scope of `createTestJobs`, `retry` resolves to `null` and `deleteFailed` to 0, since no job fails.
 * Each call of `createTestJobs` has its own enqueues and pending jobs: another instance, also with `ALL_SCOPES`, does not reach them.
 * It ignores transactions: an enqueue in a transaction that rolls back stays recorded.
 */
export function createTestJobs(options: TestJobsOptions = {}): TestJobs {
  const { scope } = options
  const calls: TestJobCall[] = []
  const captures = new Set<TestJobCall[]>()
  const pending: PendingJob[] = []
  let lastId = 0

  /**
   * Returns the function that finds the pending job of a key of the task and scope of an enqueue that takes its enqueues, among the pending jobs before it: by a map for several jobs with a key, built once per call, and by a search for one, so that an enqueue of one job does not index every pending job.
   * The jobs of one call have distinct keys, so none merges into another of the call.
   */
  const createPendingJobLookup = (
    task: string,
    jobScope: string,
    jobs: readonly { key?: string | null }[],
  ): ((key: string) => PendingJob | undefined) => {
    const isOfCall = (j: PendingJob) => j.task === task && j.scope === jobScope && !j.sealed
    if (jobs.filter(params => params.key != null).length < 2) {
      return key => pending.find(j => isOfCall(j) && j.key === key)
    }
    const found = new Map<string, PendingJob>()
    for (const j of pending) if (isOfCall(j) && j.key !== null) found.set(j.key, j)
    return key => found.get(key)
  }

  /** Adds a pending job, and returns its result. */
  const addPending = (job: Omit<PendingJob, "id">): EnqueueResult => {
    const id = String(++lastId)
    pending.push({ id, ...job })
    return { id, created: true }
  }

  /**
   * Fills the jobs of `count` items of a key, at most `maxItems` in each, and returns their results: the pending job of the key that takes its enqueues, if any, and then new ones, as many full ones as needed and at most one partial one.
   */
  const fillBatch = (
    task: string,
    jobScope: string,
    key: string,
    count: number,
    maxItems: number,
    open: PendingJob | undefined,
  ): EnqueueResult[] => {
    const results: EnqueueResult[] = []
    let left = count
    if (left > 0 && open?.batch) {
      if (open.batch.maxItems === maxItems) {
        const fill = Math.min(maxItems - open.batch.items, left)
        open.batch.items += fill
        open.sealed = open.batch.items >= maxItems
        left -= fill
        results.push({ id: open.id, created: false })
      } else {
        // An open batch of another capacity, as after a deploy that changes `maxItems`, takes no more items.
        open.sealed = true
      }
    }
    while (left > 0) {
      const items = Math.min(maxItems, left)
      left -= items
      results.push(
        addPending({
          task,
          scope: jobScope,
          key,
          batch: { items, maxItems },
          sealed: items === maxItems,
        }),
      )
    }
    return results
  }

  /** Records an enqueue of a task in a scope. */
  const record = (task: string, jobScope: string, params: EnqueueJob) => {
    const call: TestJobCall = {
      task,
      scope: toJobScope(jobScope),
      payload: params.payload,
      options: toTestEnqueueOptions(params),
    }
    calls.push(call)
    for (const captured of captures) captured.push(call)
  }

  /** Returns the scope that `runJob` and `runFailure` give a job of a task without `options.job.scope`: the scope of `createTestJobs` at the call for a scoped task, as an enqueue gives it, and `null` for another task. */
  const getRunScope = (task: TaskBase<string, unknown>) => toJobScope(getEnqueueScope(task, scope))

  /** Removes the pending jobs that match a filter, and returns their count. */
  const removePending = (filter: (job: PendingJob) => boolean) => {
    let count = 0
    for (let i = pending.length - 1; i >= 0; i--) {
      if (filter(pending[i]!)) {
        pending.splice(i, 1)
        count++
      }
    }
    return count
  }

  const jobs = createJobsApi(
    {
      async enqueueMany({ task, scope: jobScope, jobs }): Promise<EnqueueResult[]> {
        // The payloads of a real enqueue are serialized, which fails for such values as a bigint before anything is recorded, also for the jobs before it.
        for (const params of jobs) encodePayload(params.payload)
        const findPendingJob = createPendingJobLookup(task, jobScope, jobs)
        const opens = jobs.map(params =>
          params.key == null ? undefined : findPendingJob(params.key),
        )
        for (const open of opens) if (open?.batch) throw createMergeError(open)
        return jobs.map((params, i) => {
          record(task, jobScope, params)
          const open = opens[i]
          if (open !== undefined) return { id: open.id, created: false }
          return addPending({ task, scope: jobScope, key: params.key ?? null, sealed: false })
        })
      },
      async enqueueItems(params): Promise<EnqueueResult[]> {
        const { task, scope: jobScope } = params
        encodePayload(params.payload)
        // The jobs API passes items only as an array, and with a key unless it is empty, which changes nothing.
        const key = params.key!
        const count = (params.payload as unknown[]).length
        const open = count > 0 ? createPendingJobLookup(task, jobScope, [params])(key) : undefined
        if (open !== undefined && !open.batch) throw createMergeError(open)
        record(task, jobScope, params)
        return fillBatch(task, jobScope, key, count, params.maxItems, open)
      },
      async retry() {
        return null
      },
      async delete({ id, scope: jobScope }) {
        return removePending(j => j.id === id && (jobScope === null || j.scope === jobScope)) > 0
      },
      async deletePending({ scope: jobScope, task, key }) {
        return removePending(
          j =>
            (jobScope === null || j.scope === jobScope) &&
            (task == null || j.task === task) &&
            (key == null || j.key === key),
        )
      },
      async deleteFailed() {
        return 0
      },
    },
    scope,
  )

  return {
    ...jobs,
    calls,
    clear() {
      calls.length = 0
      pending.length = 0
    },
    async capture<R>(action: () => R): Promise<TestJobQueries<Awaited<R>>> {
      const captured: TestJobCall[] = []
      captures.add(captured)
      try {
        const value = action()
        // Only a promise is awaited, so that the capture of an action that returns a value ends when it returns, before the code after `capture` enqueues.
        const result = (isPromiseLike(value) ? await value : value) as Awaited<R>
        return createTestJobQueries(captured, result, scope)
      } finally {
        captures.delete(captured)
      }
    },
    async runJob<P>(
      implementation: TaskImplementation<string, P>,
      ...args: [payload?: P, options?: RunJobOptions]
    ): Promise<unknown> {
      const [payload, options] = args as [P, RunJobOptions?]
      return await runJobWithScope(implementation, payload, options, () =>
        getRunScope(implementation.task),
      )
    },
    async runFailure<P>(
      implementation: TaskImplementation<string, P>,
      ...args: [payload?: P, options?: RunFailureOptions]
    ): Promise<unknown> {
      const [payload, options] = args as [P, RunFailureOptions?]
      return await runFailureWithScope(implementation, payload, options, () =>
        getRunScope(implementation.task),
      )
    },
  }
}

/**
 * Returns an error with the message and the SQLSTATE `code` of the error of the database for an enqueue that would merge into a pending job of the other kind: a job that is not a batch into a batch, or items into a job that is not a batch.
 * It is an `Error` rather than the `QueryError` of kern-db, since `kern-jobs/testing` loads no database package.
 */
function createMergeError(open: PendingJob): Error & { code: string } {
  const key = quoteLiteral(open.key!)
  const message = open.batch
    ? `The pending job of key ${key} of task ${open.task} is a batch, so a job that is not a batch cannot merge into it`
    : `The pending job of key ${key} of task ${open.task} is not a batch, so items cannot append to it`
  return Object.assign(new Error(message), { code: "55000" })
}

/** Quotes text as `quote_literal` of PostgreSQL does: in `E'…'` with doubled backslashes when it has a backslash. */
function quoteLiteral(text: string): string {
  const quoted = `'${text.replaceAll("'", "''").replaceAll("\\", "\\\\")}'`
  return text.includes("\\") ? `E${quoted}` : quoted
}

/** Returns the scope of a job as `job.scope` gives it from the scope that the table stores: `null` for the empty scope of a job without one. */
function toJobScope(scope: string): string | null {
  return scope === "" ? null : scope
}

/** Whether a value is a promise or another thenable, which `await` resolves. */
function isPromiseLike(value: unknown): value is PromiseLike<unknown> {
  return (
    (typeof value === "object" || typeof value === "function") &&
    value !== null &&
    typeof (value as { then?: unknown }).then === "function"
  )
}

/** Returns the queries of `capture` over its recorded enqueues. */
function createTestJobQueries<R>(
  calls: readonly TestJobCall[],
  result: R,
  scope: JobsOptions["scope"],
): TestJobQueries<R> {
  const callsFor = <P>(task: TaskBase<string, P>): TestJobCall<P>[] => {
    // The scope that `deletePending` of the task reaches: the empty one for an unscoped task, also of a name that a scoped definition used before.
    const taskScope = getTaskScope(task, scope)
    return calls.filter(
      call => call.task === task.name && (taskScope === null || (call.scope ?? "") === taskScope),
    ) as TestJobCall<P>[]
  }
  return {
    calls,
    result,
    callsFor,
    payloadsFor: task => callsFor(task).map(call => call.payload),
  }
}

/** Returns the options of a recorded enqueue from the parameters of the core, without the options that no layer gave. */
function toTestEnqueueOptions(params: EnqueueJob): TestEnqueueOptions {
  const options: TestEnqueueOptions = {}
  // `null` removes the key or the queue of the layers before, so it is an option that a layer gave.
  if (params.key !== undefined) options.key = params.key
  if (params.queue !== undefined) options.queue = params.queue
  if (params.maxAttempts != null) options.maxAttempts = params.maxAttempts
  if (params.delay != null) options.delay = params.delay
  if (params.runAt != null) options.runAt = params.runAt
  if (params.mode != null) options.keyMode = params.mode
  // The parameters give `maxWait` only in debounce mode, and `resetAttempts` only outside `keep`.
  if (params.maxWait != null) options.maxWait = params.maxWait
  if (params.resetAttempts != null) options.resetAttempts = params.resetAttempts
  return options
}
