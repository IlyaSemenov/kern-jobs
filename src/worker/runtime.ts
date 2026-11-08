// Runtime of a worker: checks its schema, claims jobs of its tasks into free slots, runs their handlers under kept leases, settles them, runs the reaper, and tells its readiness and state.
// Claims and settlements use the pool of the worker; heartbeats use a connection of their own.
import { createDatabase, type Database, type Driver } from "kern-db"

import { claimCycle, createClaimState } from "../core/claim"
import type { Claim, JobRow } from "../core/job"
import type { JobTable } from "../core/schema"
import type { FailOptions } from "../core/settlement"
import type { StatementTarget } from "../core/statement"
import { type Backoff, defaultBackoff } from "../task/backoff"
import { delayRange, isDelay } from "../task/delay"
import { RetryError, TerminalError } from "../task/errors"

import type { WorkerEvent } from "./events"
import { type HandlerAbort, type KeptLease, startHeartbeat } from "./heartbeat"
import { type Loop, maxInterval, monotonicNow, startLoop } from "./loop"
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
}

/** Options of `Worker.stop`. */
export interface StopOptions {
  /** Seconds from the stop during which running handlers may end before their signals are aborted; 5 by default. */
  abortAfter?: number | undefined
  /** Seconds from the stop after which it rejects, leaving the jobs that still run to their leases; without it, `stop` waits as long as they run. */
  timeout?: number | undefined
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
  heartbeatSource?: Pick<Driver, "acquire">
  tasks: Record<string, WorkerTask>
  /** Jobs that run at once; a job takes its slot from its claim until its settlement ends. */
  concurrency: number
  /** Name of the worker in claimed rows. */
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
   * Resolves once the worker is ready: it has checked its schema and has taken jobs, or found none, while its heartbeat works, so its database, its schema, and a connection of its pool beside the one of its heartbeat work.
   * Rejects if `stop` begins before the worker is ready.
   */
  readonly ready: Promise<void>
  /** Current state of the worker. */
  readonly state: WorkerState
  /**
   * Stops the worker: it takes no more jobs, lets the running handlers end until `abortAfter`, then aborts their signals, and resolves once they end and the worker has recorded their outcomes or given that up, such as without a connection; a job whose outcome is not recorded returns to pending once its lease expires, or fails on its last attempt.
   * A job whose handler throws after the abort, other than a `TerminalError` or a `RetryError`, returns to pending at once without spending its attempt.
   * At `timeout`, it stops waiting and rejects: the worker starts no more operations, a handler that still runs is no longer settled, and its job returns once its lease expires; the statements in flight are not cancelled.
   * A later call returns the promise of the first one.
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
 * Whether a handler threw a `TerminalError` or a `RetryError`, which decides its outcome also after an abort.
 * A thrown value whose class `instanceof` cannot tell, such as a proxy whose trap throws, decides nothing.
 */
function isHandlerDecision(error: unknown): boolean {
  try {
    return error instanceof TerminalError || error instanceof RetryError
  } catch {
    return false
  }
}

/**
 * Builds the options of the fail of a job whose handler threw `error`: a `TerminalError` fails it for good, a `RetryError` retries it after its delay, and any other error after the backoff of its task.
 * A backoff that throws or gives a delay outside the range of `isDelay`, which keeps SQL from rejecting the fail, is reported, and the job retries with the default backoff, so that it is still settled.
 */
function toFailOptions(
  task: WorkerTask,
  claim: Claim,
  error: unknown,
  onBackoffError: (error: unknown) => void,
): FailOptions {
  const message = toErrorMessage(error)
  const attempt = claim.job.attempts
  try {
    if (error instanceof TerminalError) return { error: message, terminal: true, backoff: 0 }
    const backoff =
      error instanceof RetryError ? error.delay : (task.backoff ?? defaultBackoff)(attempt)
    if (isDelay(backoff)) return { error: message, backoff }
    onBackoffError(
      new RangeError(
        `The delay of the retry of task ${claim.job.task} is ${String(backoff)}, not ${delayRange}`,
      ),
    )
  } catch (e) {
    // `instanceof` runs code of the thrown value, such as a trap of a proxy, and the backoff runs code of the task.
    onBackoffError(e)
  }
  return { error: message, backoff: defaultBackoff(attempt) }
}

/** Starts the runtime of a worker; it trusts its options, which `startWorker` checks. */
export function startRuntime(o: RuntimeOptions): Worker {
  const { id, lease } = o
  const leases = Object.fromEntries(
    Object.entries(o.tasks).map(([name, task]) => [name, task.lease ?? lease]),
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

  // Database of the settlements: no other code runs transactions in it, so a fail transaction never nests in another one.
  // The worker never closes it, since that would close the driver.
  const database = createDatabase({
    driver: o.driver,
    onReleaseError: error => emit({ type: "release-error", error }),
  })

  interface Execution {
    lease: KeptLease
    done: Promise<void>
  }
  const executions = new Set<Execution>()
  let stopping = false
  // The timeout of `stop` passed: the worker starts no operation any more.
  let givenUp = false
  let waitingForHeartbeat = false
  // The worker has checked its schema and finished a claim cycle, whose statement used a connection of the pool while the heartbeat held its own.
  let isReady = false
  let isStopped = false
  const ready = Promise.withResolvers<void>()
  // An application that never awaits `ready` must not get an unhandled rejection when the worker stops before it is ready.
  ready.promise.catch(() => {})

  const heartbeat = startHeartbeat({
    source: o.heartbeatSource ?? o.driver,
    table: o.table,
    interval,
    margin,
    onEvent: emit,
    onSuccess: () => {
      if (waitingForHeartbeat) claimLoop.wake()
    },
  })

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
        options: toFailOptions(task, claim, new Error(lateClaimError), e =>
          emit({ type: "backoff-error", claim, error: e }),
        ),
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
      kept.endHandler()
      const reason = kept.signal.aborted ? (kept.signal.reason as HandlerAbort).reason : null
      const interrupted = failed && reason === "stop" && !isHandlerDecision(error)
      emit({ type: "end", claim, failed, error, aborted: reason, interrupted })
      if (!failed) return { type: "complete" }
      if (interrupted) return { type: "requeue" }
      return {
        type: "fail",
        options: toFailOptions(task, claim, error, e =>
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
        database,
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
  function startReaperOnce() {
    if (o.reaper !== false && !stopping) {
      reaper ??= startReaper(o.driver, o.table, { ...o.reaper, onEvent: emit })
    }
  }
  if (schemaChecked) startReaperOnce()

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
        startReaperOnce()
      }
      const free = o.concurrency - executions.size
      if (stopping || free <= 0) return
      // A worker that cannot heartbeat could not keep a new lease, and every job it claimed would spend an attempt.
      waitingForHeartbeat = !heartbeat.healthy()
      if (waitingForHeartbeat) return
      try {
        await claimCycle(
          timedDriver,
          o.table,
          {
            worker: id,
            tasks: Object.keys(o.tasks),
            slots: free,
            leases,
            defaultLease: lease,
            onClaim: claim => start(claim, sentAt),
            proceed: () => !stopping && heartbeat.healthy(),
          },
          undefined,
          claimState,
        )
      } catch (error) {
        emit({ type: "claim-error", error })
        return
      }
      if (!isReady) {
        isReady = true
        ready.resolve()
      }
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
    // The claim cycle in flight is awaited apart from the abort, since it may wait for a connection of the pool that a handler holds until its abort.
    const finished = (async () => {
      await claimLoop.stop()
      while (executions.size > 0) await Promise.all([...executions].map(e => e.done))
      await reaper?.stop()
      await heartbeat.stop()
    })()
    let deadlineTimer: ReturnType<typeof setTimeout> | undefined
    try {
      if (timeout === undefined) return await finished
      let inTime = true
      const deadline = new Promise<void>(resolve => {
        deadlineTimer = setTimeout(() => {
          inTime = false
          givenUp = true
          abortAll()
          resolve()
        }, timeout * 1000)
      })
      await Promise.race([finished, deadline])
      if (inTime) return
      // Statements in flight cannot be cancelled: they end in the background, and the heartbeat releases its connection after its own.
      // Handlers that still run get no heartbeat, so their jobs return after their leases.
      void reaper?.stop()
      void heartbeat.stop()
      const jobs = executions.size > 0 ? `; jobs still running or settling: ${executions.size}` : ""
      throw new Error(`The worker did not stop within ${timeout} s${jobs}`)
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
      return stopped
    },
  }
}
