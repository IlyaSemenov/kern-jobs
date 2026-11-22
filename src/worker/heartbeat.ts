// Leases of the claims of a worker: heartbeats in a session of their own, which holds the lock of the worker id, one at a time, and the local deadlines that abort a handler whose lease the worker cannot confirm.
import { type Database, type OpenedSession, QueryError } from "kern-orm/db"

import type { ClaimRef } from "../core/job"
import { heartbeat } from "../core/lease"
import type { JobTable } from "../core/schema"
import { query, type StatementTarget } from "../core/statement"

import type { AbortReason, WorkerEvent } from "./events"
import { type Loop, maxTimerDelay, monotonicNow, startLoop } from "./loop"

/**
 * Reason of the aborted signal of a handler, `signal.reason`, which `signal.throwIfAborted()` throws and an API that takes the signal, such as `fetch`, rejects with.
 */
export class HandlerAbort extends Error {
  override readonly name = "HandlerAbort"

  constructor(
    /** Why the worker aborted the handler. */
    readonly reason: AbortReason,
  ) {
    super(`handler aborted: ${reason}`)
  }
}

/** Lease of a claim that the heartbeat keeps until `release`. */
export interface KeptLease {
  readonly claim: ClaimRef
  /** Signal of the handler, aborted with a `HandlerAbort`. */
  readonly signal: AbortSignal
  /** A heartbeat found the claim lost; the heartbeat no longer extends it. */
  readonly lost: boolean
  /** Monotonic time in milliseconds until which the lease is confirmed. */
  readonly confirmedUntil: number
  /** Aborts the handler, unless it has ended. */
  abort(reason: AbortReason): void
  /** Marks the end of the handler: the lease is kept for the settlement, and nothing is aborted any more. */
  endHandler(): void
  /** Stops keeping the lease. */
  release(): void
}

/** Options of the heartbeat of a worker. */
export interface HeartbeatOptions {
  /** Database of the worker in which the heartbeat opens its session; its `onReleaseError` receives the failures to release the connection of the session. */
  database: Database
  /** Advisory lock that the session holds while the worker runs, and the message of the error of a round that cannot take it. */
  lock: { key: string; busyMessage: string }
  table: JobTable
  /** Seconds between heartbeats. */
  interval: number
  /** Seconds before the end of an unconfirmed lease at which its handler is aborted. */
  margin: number
  onEvent(event: WorkerEvent): void
  /** Called after each successful heartbeat. */
  onSuccess(): void
}

/** Heartbeat of a worker. */
export interface Heartbeat {
  /** Starts keeping the lease of a claim, confirmed until `since` plus `lease` seconds of monotonic time. */
  keep(claim: ClaimRef, lease: number, since: number): KeptLease
  /**
   * Whether the worker can keep new leases: the latest heartbeat succeeded, so its session held the lock then, and none has been in flight for longer than an interval.
   */
  healthy(): boolean
  /**
   * Runs `callback` with a target of the statements of the session of the heartbeat while the session holds the lock, so that the lock is released only after `callback` ends.
   * Rejects without calling `callback` once the stop began, without a session, or when another session holds the lock.
   */
  runLocked<T>(callback: (target: StatementTarget) => Promise<T>): Promise<T>
  /**
   * Stops the heartbeats and the keeping of every lease, and resolves once the heartbeat in flight ends and the session is closed; a later call returns the same promise.
   */
  stop(): Promise<void>
}

interface Entry extends KeptLease {
  lease: number
  lost: boolean
  confirmedUntil: number
  confirm(until: number): void
}

/**
 * Starts the heartbeat of a worker.
 * It sends one heartbeat at a time for every kept lease that is not lost, or `select 1` without one, so that a broken connection is noticed before the worker claims.
 * It sends them in a session that it opens with `openSession` of its database, which keeps one connection across the rounds.
 * Each round takes the lock of `o.lock` first, which kern-db answers without a statement in a session that holds it, and the session holds it until it is closed; a round whose lock another session holds fails with the message of `o.lock`, closes the session, and sends no heartbeat, so that the worker takes no jobs meanwhile.
 * When a round fails with an open session, a rejected statement keeps the session, and any other error, such as of a lost connection, closes it, which does not return a connection that may be broken to the pool; the next round opens a session when it has none.
 */
export function startHeartbeat(o: HeartbeatOptions): Heartbeat {
  const entries = new Set<Entry>()
  let session: OpenedSession | null = null
  let succeeded = false
  let failed = false
  let inFlightSince: number | null = null
  let stopping: Promise<void> | undefined
  const { database } = o
  /** Takes the lock in the session of the caller, and throws when another session holds it. */
  async function takeLock() {
    if (!(await database.tryLock(o.lock.key))) throw new Error(o.lock.busyMessage)
  }

  async function beat() {
    const sentAt = monotonicNow()
    inFlightSince = sentAt
    try {
      session ??= await database.openSession()
      // A session that opens after the stop is closed by the stop, which waits for this round.
      if (stopping) return
      await session.run(takeLock)
      // A stop that began while the lock was taken sends no heartbeat; it waits for this round and then closes the session.
      if (stopping) return
      const kept = [...entries].filter(e => !e.lost)
      if (kept.length === 0) await session.run(() => query(database, "select 1"))
      else {
        o.onEvent({ type: "heartbeat", claims: kept.map(e => e.claim.claimId) })
        const claims = kept.map(e => ({ id: e.claim.id, claimId: e.claim.claimId, lease: e.lease }))
        const result = await session.run(() => heartbeat(database, o.table, { claims }))
        const confirmed = new Set(result.confirmed)
        const busy = new Set(result.busy)
        for (const e of kept) {
          if (!entries.has(e)) continue
          // The heartbeat confirms a lease from the moment it was sent, before the server extended it.
          if (confirmed.has(e.claim.claimId)) e.confirm(sentAt + e.lease * 1000)
          else if (!busy.has(e.claim.claimId)) {
            e.lost = true
            e.abort("lost")
          }
        }
      }
      succeeded = true
      // A slow heartbeat that succeeded is no longer in flight for the observers of its success.
      inFlightSince = null
      if (failed) {
        failed = false
        o.onEvent({ type: "heartbeat-recovered" })
      }
      o.onSuccess()
    } catch (error) {
      succeeded = false
      failed = true
      o.onEvent({ type: "heartbeat-error", error })
      if (!(error instanceof QueryError) && session) {
        const lost = session
        session = null
        await lost.close()
      }
    } finally {
      inFlightSince = null
    }
  }

  const loop: Loop = startLoop(beat, {
    interval: o.interval,
    onError: error => o.onEvent({ type: "heartbeat-error", error }),
  })

  return {
    keep(claim, lease, since) {
      const controller = new AbortController()
      let timer: ReturnType<typeof setTimeout> | undefined
      let handlerEnded = false
      const schedule = () => {
        clearTimeout(timer)
        if (handlerEnded) return
        const delay = entry.confirmedUntil - o.margin * 1000 - monotonicNow()
        if (delay <= 0) return entry.abort("deadline")
        // A timer fires at once for a delay above its maximum, so a longer one is scheduled again when it fires.
        timer = setTimeout(schedule, Math.min(delay, maxTimerDelay))
      }
      const entry: Entry = {
        claim,
        lease,
        signal: controller.signal,
        lost: false,
        confirmedUntil: since + lease * 1000,
        abort(reason) {
          if (reason === "lost") clearTimeout(timer)
          if (handlerEnded || controller.signal.aborted) return
          controller.abort(new HandlerAbort(reason))
          o.onEvent({ type: "abort", claim, reason })
        },
        confirm(until) {
          entry.confirmedUntil = Math.max(entry.confirmedUntil, until)
          schedule()
        },
        endHandler() {
          handlerEnded = true
          clearTimeout(timer)
        },
        release() {
          clearTimeout(timer)
          entries.delete(entry)
        },
      }
      entries.add(entry)
      schedule()
      return entry
    },
    runLocked(callback) {
      const held = session
      if (stopping || !held) return Promise.reject(new Error("The heartbeat holds no lock"))
      return held.run(async () => {
        await takeLock()
        return await callback(database)
      })
    },
    healthy() {
      return (
        succeeded && (inFlightSince == null || monotonicNow() - inFlightSince < o.interval * 1000)
      )
    },
    stop() {
      stopping ??= (async () => {
        // Leases are no longer kept from the call, also when the heartbeat in flight returns later.
        for (const e of entries) e.release()
        await loop.stop()
        if (session) {
          const held = session
          session = null
          await held.close()
        }
      })()
      return stopping
    },
  }
}
