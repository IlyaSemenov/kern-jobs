// Runtime of a worker: checks its schema, returns the jobs of a previous worker of its id, claims jobs of its tasks into free slots, runs their handlers under kept leases, settles them, runs the reaper, and tells its readiness and state.
// Claims and settlements use the pool of the worker; heartbeats use a connection of their own.
import { createDatabase, type Database, type Driver } from "kern-orm/db"

import type { WorkerSchedule } from "../schedule/schedule"
import type { JobRow } from "../sql/job"
import type { JobTable } from "../sql/job-table"
import { labelSql } from "../sql/statement"
import { type Backoff, defaultBackoff } from "../task/backoff"
import { delayRange, isDelay } from "../task/delay"

import { type Claim, claimCycle, createClaimState } from "./db/claim"
import { bindWorkerDb } from "./db/db"
import { reap } from "./db/lease"
import { query, type StatementTarget } from "./db/query"
import { evaluateSchedule } from "./db/schedule"
import type { FailOptions } from "./db/settlement"
import type { WorkerEvent } from "./events"
import { type HandlerAbort, type KeptLease, startHeartbeat } from "./heartbeat"
import { type Loop, maxInterval, monotonicNow, startLoop } from "./loop"
import { classifyThrown, type Thrown, toHandlerOutcome } from "./outcome"
import { startReaper } from "./reaper"
import { settle, type SettleAction } from "./settle"

/** Task that a worker runs: the handler of its jobs, its lease, and its backoff. */
export interface WorkerTask {
  /**
   * Runs a job; the job completes when it returns and fails when it throws.
   * The signal is aborted with a `HandlerAbort` when the worker loses or cannot confirm the lease of the job, or stops.
   */
  handler(job: JobRow, context: { signal: AbortSignal }): unknown
  /** Lease in seconds; the lease of the worker when omitted. */
  lease?: number | undefined
  /** `defaultBackoff` when omitted. */
  backoff?: Backoff | undefined
  /** Attempt budget of the failure jobs of the task, which each claim of the task stamps on its job, for a task that has a failure task; no failure jobs when omitted. */
  failureMaxAttempts?: number | undefined
}

/** Options of `Worker.stop`. */
export interface StopOptions {
  /** Seconds from the stop during which running handlers may end before their signals are aborted; 5 by default. */
  abortAfter?: number | undefined
  /** Seconds from the stop after which it rejects, leaving the jobs that still run to their leases; without it, `stop` waits as long as they run. */
  timeout?: number | undefined
  /** When true, `stop` stops waiting at once and rejects, as at `timeout`, also when another call has begun the stop; `abortAfter` and `timeout` then change nothing. */
  force?: boolean | undefined
}

/** Options of the runtime of a worker, which `startWorker` checks and fills from the options of the application. */
export interface RuntimeOptions {
  /** Pool of claims and settlements, which needs a connection for each settlement that runs at once. */
  driver: Driver
  /** Job table of the jobs that the worker runs. */
  table: JobTable
  /**
   * Pool from which the heartbeat reserves its connection for the life of the worker; `driver` when omitted, which then needs at least two connections.
   */
  heartbeatSource?: Driver
  tasks: Record<string, WorkerTask>
  /** Jobs that run at once; a job takes its slot from its claim until its settlement ends. */
  concurrency: number
  /** Name of the worker in claimed rows, unique among the running workers of its installation, which the heartbeat holds the lock of. */
  id: string
  /** Lease in seconds of tasks without one. */
  lease: number
  /**
   * Seconds between heartbeats; a third of the shortest lease when omitted, up to the longest interval of a loop, since heartbeats more often than a lease needs are safe.
   */
  heartbeatInterval?: number | undefined
  /** Seconds before the end of an unconfirmed lease at which the handler is aborted; a sixth of the shortest lease when omitted. */
  abortMargin?: number | undefined
  /** Seconds between claim cycles while slots are free. */
  pollInterval: number
  /** Recurring enqueues evaluated independently of free handler slots. */
  schedules?: readonly WorkerSchedule[] | undefined
  /** Reaper of the worker, seconds between its rounds and jobs per batch, or `false` to run none. */
  reaper: false | { interval: number; limit: number }
  /**
   * Checks the schema of the job table on the database of the worker, and rejects while the worker must not use it, such as before its migrations are applied.
   * The worker claims and reaps nothing until it resolves, and calls it again after the poll interval; without it, the worker trusts the schema.
   */
  checkSchema?(database: Database): Promise<void>
  /** Receives every event synchronously; an error it throws is ignored. */
  onEvent?(event: WorkerEvent): void
}

/**
 * State of a worker:
 * - `starting`: the worker has not yet checked its schema or taken jobs, such as before `migrateJobs` or while it waits for a connection;
 * - `ready`: the worker has become ready, and its heartbeat is healthy; waiting for a connection of a busy pool does not change this state;
 * - `unhealthy`: the latest heartbeat failed or is late, such as after the loss of the database: the worker takes no jobs until a heartbeat succeeds;
 * - `stopping`: `stop` has begun;
 * - `stopped`: `stop` has resolved or rejected.
 */
export type WorkerState = "starting" | "ready" | "unhealthy" | "stopping" | "stopped"

/** Running worker, which `startWorker` returns. */
export interface Worker {
  /** Name of the worker, which the jobs that it runs store. */
  readonly id: string
  /**
   * Resolves once the worker is ready: it has checked its schema and run a claim cycle, or a statement without tasks, on its pool, and its heartbeat works.
   * So its database, its schema, and a connection of its pool beside the one of its heartbeat work.
   * Rejects if `stop` begins before the worker is ready.
   */
  readonly ready: Promise<void>
  /** Current state of the worker. */
  readonly state: WorkerState
  /**
   * Stops the worker: it takes no more jobs, lets the running handlers end until `abortAfter`, then aborts their signals, and resolves once they end and the worker has recorded their outcomes or given that up, such as without a connection; a job whose outcome is not recorded returns to pending once its lease expires, or fails on its last attempt.
   * A job whose handler throws after the abort, other than a `TerminalError` or a `RetryError`, returns to pending at once without spending its attempt, unless the stop has given up by then.
   * It starts no further check of a schedule and waits for the one in flight.
   * At `timeout`, or at once with `force`, it stops waiting and rejects: the worker starts no more operations, a handler that still runs is no longer settled, and its job returns once its lease expires; the statements in flight are not cancelled, so a check of a schedule in flight may still enqueue its job.
   * Every call throws `TypeError` or `RangeError` for a wrong option before it acts; a later call with valid options returns the promise of the first one, and its `abortAfter` and `timeout` change nothing, while its `force` gives up the stop in progress.
   */
  stop(options?: StopOptions): Promise<void>
}

/**
 * Returns the message of a thrown value, also of one whose inspection or string conversion throws, such as `Object.create(null)` or an object with a throwing getter.
 * U+0000 becomes the text `\u0000`, since PostgreSQL rejects it in a text value, and the fail would never apply.
 */
function toErrorMessage(error: unknown): string {
  let message: string
  try {
    message = error instanceof Error ? String(error.message) : String(error)
  } catch {
    return "unprintable error"
  }
  return message.replaceAll("\0", "\\u0000")
}

/** Error of a job whose claim returned after the deadline of its lease, so that the worker did not start its handler. */
const lateClaimError = "lease confirmation deadline passed before the handler started"

/**
 * Builds the options of the fail of a job whose handler threw `error`, by its classification `thrown`: a `TerminalError` fails it for good, with the reason `terminal-error`, a `RetryError` retries it after its delay, and any other error after the backoff of its task, with the reason of a fail without one, `handler-error`.
 * When it gets no valid delay, it reports the error and takes the default backoff, so that the job is still settled: what was thrown by the backoff or by the read of the class of the thrown value or of the delay of a `RetryError`, or a `RangeError` for a delay outside the range of `isDelay`, which keeps SQL from rejecting the fail.
 * The fail uses the delay only if the job returns to pending.
 */
function toFailOptions(
  task: WorkerTask,
  claim: Claim,
  error: unknown,
  thrown: Thrown,
  onBackoffError: (error: unknown) => void,
): FailOptions {
  const message = toErrorMessage(error)
  const attempt = claim.job.attempts
  if (thrown.type === "terminal") return { error: message, reason: "terminal-error", backoff: 0 }
  if ("failure" in thrown) onBackoffError(thrown.failure)
  else {
    try {
      const backoff =
        thrown.type === "retry" ? thrown.delay : (task.backoff ?? defaultBackoff)(attempt)
      if (isDelay(backoff)) return { error: message, backoff }
      onBackoffError(
        new RangeError(
          `The delay of the retry of task ${claim.job.task} is ${String(backoff)}, not ${delayRange}`,
        ),
      )
    } catch (e) {
      // The backoff runs code of the task.
      onBackoffError(e)
    }
  }
  return { error: message, backoff: defaultBackoff(attempt) }
}

/**
 * Returns the advisory lock of a worker id in the schema of a job table, which the heartbeat holds while the worker runs, and the message of the error of a heartbeat while another worker holds it.
 * The key names kern-jobs, since an application takes advisory locks of its own in the same space, and the schema name, which identifies the installation within the database, since workers of different installations may share an id; JSON keeps a separator in the schema or the id from making two keys equal.
 */
function toWorkerLock(table: JobTable, id: string): { key: string; busyMessage: string } {
  return {
    key: `kern-jobs:worker:${JSON.stringify([table.schemaName, id])}`,
    busyMessage: `Worker id ${id} is in use in schema ${table.schemaName}; this worker takes no jobs until the id is free`,
  }
}

/** Maximum number of jobs that one batch of the return at the start of a worker returns. */
const recoveryLimit = 100

/** Rejection of a statement of the return of the jobs of the id after the stop began, which ends the return without an error. */
const recoveryStopped = new Error("The worker stopped")

/** Starts the runtime of a worker; it trusts its options, which `startWorker` checks. */
export function startRuntime(o: RuntimeOptions): Worker {
  const { id, lease } = o
  const taskNames = Object.keys(o.tasks)
  const leases = Object.fromEntries(
    Object.entries(o.tasks).map(([name, task]) => [name, task.lease ?? lease]),
  )
  const failureMaxAttempts = Object.fromEntries(
    Object.entries(o.tasks).flatMap(([name, task]) =>
      task.failureMaxAttempts === undefined ? [] : [[name, task.failureMaxAttempts]],
    ),
  )
  const shortest = Math.min(lease, ...Object.values(leases))
  const interval = o.heartbeatInterval ?? Math.min(shortest / 3, maxInterval)
  const margin = o.abortMargin ?? shortest / 6
  // A lease must outlive a missed heartbeat and the margin, or handlers would be aborted between heartbeats.
  if (interval + margin >= shortest)
    throw new Error(
      `The heartbeat interval ${interval} s and the abort margin ${margin} s must be shorter than the lease ${shortest} s`,
    )
  const emit = (event: WorkerEvent) => {
    try {
      o.onEvent?.(event)
    } catch {
      // Observation must not break the worker.
    }
  }

  // Database for independent worker transactions, outside the application transaction context, and for the session of the heartbeat when it has no pool of its own.
  // The worker never closes it, since that would close the driver.
  const onReleaseError = (error: unknown) => emit({ type: "release-error", error })
  const database = createDatabase({ driver: o.driver, onReleaseError })
  const db = bindWorkerDb(database, o.table.schemaName)

  interface Execution {
    lease: KeptLease
    done: Promise<void>
  }
  const executions = new Set<Execution>()
  let stopping = false
  // The timeout of `stop` passed, or `stop` was forced: the worker starts no operation any more.
  let givenUp = false
  // Gives up the stop in progress with the message of its rejection; a no-op until `stop` begins.
  let giveUp: (failure: string) => void = () => {}
  let waitingForHeartbeat = false
  // The worker has checked its schema and finished a claim cycle, or a statement of a worker without tasks, on a connection of the pool beside the one of the heartbeat.
  let poolProven = false
  // The pool is proven and the heartbeat was healthy then or later.
  let isReady = false
  let isStopped = false
  const ready = Promise.withResolvers<void>()
  // An application that never awaits `ready` must not get an unhandled rejection when the worker stops before it is ready.
  ready.promise.catch(() => {})

  const heartbeat = startHeartbeat({
    database: o.heartbeatSource
      ? createDatabase({ driver: o.heartbeatSource, onReleaseError })
      : database,
    lock: toWorkerLock(o.table, id),
    table: o.table,
    interval,
    margin,
    onEvent: emit,
    onSuccess: () => {
      // The heartbeat may recover while every slot runs a job, when no claim cycle would follow.
      becomeReady()
      if (waitingForHeartbeat) claimLoop.wake()
    },
  })

  /** Resolves `ready` once the pool is proven while the heartbeat is healthy. */
  function becomeReady() {
    if (isReady || !poolProven || !heartbeat.healthy()) return
    isReady = true
    ready.resolve()
  }

  /** Starts the execution of a claim: its handler, or no handler after `stop` began or after the deadline of its lease. */
  function start(claim: Claim, since: number) {
    // The job returns once its lease expires, as after a claim statement whose connection broke.
    if (givenUp) return
    const task = o.tasks[claim.job.task]!
    const kept = heartbeat.keep(claim, leases[claim.job.task]!, since)
    if (stopping) {
      // No code of the handler runs, so the attempt is given back; stop wins over a passed deadline.
      kept.endHandler()
      return execute(claim, kept, async () => ({ type: "requeue" }))
    }
    if (kept.signal.aborted) {
      // The handler would start aborted.
      // The attempt counts, so that a claim statement that is always this slow fails the job in the end rather than returns it forever.
      kept.endHandler()
      emit({ type: "late-claim", claim })
      return execute(claim, kept, async () => ({
        type: "fail",
        options: {
          ...toFailOptions(task, claim, new Error(lateClaimError), { type: "other" }, e =>
            emit({ type: "backoff-error", claim, error: e }),
          ),
          reason: "late-claim",
        },
      }))
    }
    execute(claim, kept, async () => {
      emit({ type: "start", claim })
      let failed = false
      let error: unknown
      try {
        await task.handler(claim.job, { signal: kept.signal })
      } catch (e) {
        failed = true
        error = e
      }
      // The handler has ended before any code of the thrown value runs, so that such code, such as a trap of a proxy that stops the worker, cannot abort its signal.
      kept.endHandler()
      const aborted = kept.signal.aborted ? (kept.signal.reason as HandlerAbort).reason : null
      // The thrown value is classified once, before the report, whose receiver gets the value and can change it: the report and the settlement both use this classification.
      const thrown = failed ? classifyThrown(error) : null
      const { attempts: attempt, max_attempts: maxAttempts } = claim.job
      const outcome = toHandlerOutcome({ thrown, aborted, attempt, maxAttempts })
      emit({ type: "end", claim, outcome, error, aborted })
      if (!thrown) return { type: "complete" }
      if (outcome === "requeue") return { type: "requeue" }
      return {
        type: "fail",
        options: toFailOptions(task, claim, error, thrown, e =>
          emit({ type: "backoff-error", claim, error: e }),
        ),
      }
    })
  }

  /** Runs an execution, which takes a slot until the settlement of the action that `run` returns ends. */
  function execute(claim: Claim, kept: KeptLease, run: () => Promise<SettleAction>) {
    const execution: Execution = { lease: kept, done: Promise.resolve() }
    executions.add(execution)
    execution.done = (async () => {
      const action = await run()
      const outcome = await settle({
        db,
        table: o.table,
        claim,
        lease: kept,
        action,
        retryDelay: Math.min(0.1, interval),
        maxRetryDelay: interval,
        // A handler that ends after the timeout of stop is not settled, and a settlement is not retried after it.
        proceed: () => !givenUp,
        onEvent: emit,
      })
      emit({ type: "settled", claim, outcome })
    })()
      .catch(error => emit({ type: "error", error }))
      .finally(() => {
        kept.release()
        executions.delete(execution)
        claimLoop.wake()
      })
  }

  // Claims start their leases from the send time of their own statement: the statements of a cycle run one after another, and a slow one must not shorten the leases of the claims after it.
  let sentAt = 0
  const timedDriver: StatementTarget = {
    execute: statement => {
      sentAt = monotonicNow()
      return o.driver.execute(statement)
    },
  }

  // The reaper starts with the first claim cycle, once the schema is checked.
  let schemaChecked = o.checkSchema === undefined
  let reaper: Loop | undefined
  let scheduler: Loop | undefined
  function startMaintenance() {
    const schedules = o.schedules
    if (schedules?.length && !stopping) {
      scheduler ??= startLoop(
        async () => {
          for (const schedule of schedules) {
            if (stopping) break
            try {
              await evaluateSchedule(db, o.table, schedule, { proceed: () => !givenUp })
            } catch (error) {
              emit({ type: "schedule-error", id: schedule.id, error })
            }
          }
        },
        { interval: 15, onError: error => emit({ type: "error", error }) },
      )
    }
    if (o.reaper !== false && !stopping) {
      reaper ??= startReaper(o.driver, o.table, { ...o.reaper, onEvent: emit })
    }
  }
  if (schemaChecked) startMaintenance()

  /**
   * Returns the active jobs of the id to pending, or failed on their last attempt, without waiting for their leases, in batches.
   * The worker calls it before its first claim, while its heartbeat holds the lock of the id: the session of the worker that claimed them has ended, such as of a process killed before a restart.
   * Each batch runs in the session of the heartbeat, so that the lock outlives it: once the session closes, such as at a stop that gives up, a worker of the id that starts cannot find a batch of this one returning the jobs that it claims.
   * A batch that returns a row is followed by another, which tries again the rows whose keys a producer held, and a batch that returns none ends the return.
   * Once the stop begins, it sends no further statement, also within a batch, and the jobs that it did not return wait for a worker of the id or for their leases.
   */
  async function recoverJobs() {
    while (!stopping) {
      let jobs
      try {
        jobs = await heartbeat.runLocked(session =>
          reap(
            {
              execute: statement =>
                stopping ? Promise.reject(recoveryStopped) : session.execute(statement),
            },
            o.table,
            { worker: id, limit: recoveryLimit },
          ),
        )
      } catch (error) {
        if (error === recoveryStopped) return
        throw error
      }
      if (jobs.length === 0) return
      emit({ type: "recover", jobs })
    }
  }
  let recovered = false

  const claimState = createClaimState()
  const claimLoop: Loop = startLoop(
    async () => {
      if (stopping) return
      if (!schemaChecked) {
        try {
          await o.checkSchema!(database)
        } catch (error) {
          emit({ type: "schema-error", error })
          return
        }
        schemaChecked = true
        startMaintenance()
      }
      const free = o.concurrency - executions.size
      if (stopping || free <= 0) return
      // A worker that cannot heartbeat could not keep a new lease, and every job it claimed would spend an attempt.
      waitingForHeartbeat = !heartbeat.healthy()
      if (waitingForHeartbeat) return
      try {
        if (!recovered) {
          await recoverJobs()
          recovered = true
          // The claim cycle does not check the stop and the heartbeat before its first statement, and both may have changed during the return.
          if (stopping) return
          waitingForHeartbeat = !heartbeat.healthy()
          if (waitingForHeartbeat) return
        }
        // A worker without tasks claims nothing; one statement proves a connection of the pool beside the heartbeat's.
        if (taskNames.length === 0) {
          if (!poolProven) await query(database, labelSql("worker:ping", "select 1"))
        } else {
          await claimCycle(
            timedDriver,
            o.table,
            {
              worker: id,
              tasks: taskNames,
              slots: free,
              leases,
              defaultLease: lease,
              failureMaxAttempts,
              onClaim: claim => start(claim, sentAt),
              proceed: () => !stopping && heartbeat.healthy(),
            },
            undefined,
            claimState,
          )
        }
      } catch (error) {
        emit({ type: "claim-error", error })
        return
      }
      poolProven = true
      // The heartbeat may have failed while the statement ran; then the next successful heartbeat makes the worker ready.
      becomeReady()
    },
    { interval: o.pollInterval, onError: error => emit({ type: "claim-error", error }) },
  )

  /** Stops the worker by the phases of `Worker.stop`. */
  async function stopRuntime(abortAfter: number, timeout: number | undefined) {
    stopping = true
    // A worker that is already ready keeps `ready` resolved.
    ready.reject(new Error("The worker stopped before it was ready"))
    const abortAll = () => {
      for (const e of executions) e.lease.abort("stop")
    }
    // At the timeout, the abort follows the decision to give up, so that a handler that ends on it is not settled.
    const abortTimer =
      timeout === undefined || abortAfter < timeout
        ? setTimeout(abortAll, abortAfter * 1000)
        : undefined
    let isFinished = false
    // The claim cycle in flight is awaited apart from the abort, since it may wait for a connection of the pool that a handler holds until its abort.
    const finished = (async () => {
      await Promise.all([claimLoop.stop(), scheduler?.stop()])
      while (executions.size > 0) await Promise.all([...executions].map(e => e.done))
      await reaper?.stop()
      await heartbeat.stop()
      isFinished = true
    })()
    let failure: string | undefined
    const gaveUp = new Promise<void>(resolve => {
      giveUp = message => {
        // A stop that has finished or given up already keeps its outcome.
        if (isFinished || givenUp) return
        givenUp = true
        failure = message
        // The reaper and the heartbeat stop within the call, so that a connection that the heartbeat acquires after it sends no statement.
        // A statement in flight cannot be cancelled: it ends in the background, and the heartbeat releases its connection after its own.
        // Handlers that still run get no heartbeat, so their jobs return after their leases.
        void reaper?.stop()
        void heartbeat.stop()
        abortAll()
        resolve()
      }
    })
    const deadlineTimer =
      timeout === undefined
        ? undefined
        : setTimeout(() => giveUp(`The worker did not stop within ${timeout} s`), timeout * 1000)
    try {
      await Promise.race([finished, gaveUp])
      if (failure === undefined) return
      const jobs = executions.size > 0 ? `; jobs still running or settling: ${executions.size}` : ""
      throw new Error(`${failure}${jobs}`)
    } finally {
      clearTimeout(abortTimer)
      clearTimeout(deadlineTimer)
    }
  }

  let stopped: Promise<void> | undefined
  return {
    id,
    ready: ready.promise,
    get state() {
      if (isStopped) return "stopped"
      if (stopping) return "stopping"
      if (!isReady) return "starting"
      return heartbeat.healthy() ? "ready" : "unhealthy"
    },
    stop(options = {}) {
      stopped ??= stopRuntime(options.abortAfter ?? 5, options.timeout).finally(() => {
        isStopped = true
      })
      // The promise is kept before the abort, so that a call of `stop` from a listener of the abort returns it.
      if (options.force) giveUp("The worker stop was forced")
      return stopped
    },
  }
}
