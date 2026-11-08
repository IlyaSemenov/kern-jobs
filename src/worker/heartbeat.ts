// Leases of the claims of a worker: heartbeats on a connection of their own, one at a time, and the local deadlines that abort a handler whose lease the worker cannot confirm.
import { type Connection, type Driver, QueryError } from "kern-db"

import type { ClaimRef } from "../core/job"
import { heartbeat } from "../core/lease"
import type { JobTable } from "../core/schema"
import { query } from "../core/statement"

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
  /** Pool from which the heartbeat reserves its connection. */
  source: Pick<Driver, "acquire">
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
   * Whether the worker can keep new leases: the latest heartbeat succeeded, and none has been in flight for longer than an interval.
   */
  healthy(): boolean
  /**
   * Stops the heartbeats and the keeping of every lease, and resolves once the heartbeat in flight ends and the connection is released; a later call returns the same promise.
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
 * After a connection error it releases the connection, which the driver does not return to the pool while it may be broken, and reserves another one for the next heartbeat; a rejected statement keeps the connection.
 */
export function startHeartbeat(o: HeartbeatOptions): Heartbeat {
  const entries = new Set<Entry>()
  let connection: Connection | null = null
  let succeeded = false
  let failed = false
  let inFlightSince: number | null = null
  let stopping: Promise<void> | undefined
  /** Releases a connection and reports a release that fails as `release-error`. */
  async function releaseConnection(released: Connection) {
    try {
      await released.release()
    } catch (error) {
      o.onEvent({ type: "release-error", error })
    }
  }

  async function beat() {
    const sentAt = monotonicNow()
    inFlightSince = sentAt
    try {
      connection ??= await o.source.acquire()
      // A connection that arrives after the stop is released by the stop, which waits for this round.
      if (stopping) return
      const kept = [...entries].filter(e => !e.lost)
      if (kept.length === 0) await query(connection, "select 1")
      else {
        o.onEvent({ type: "heartbeat", claims: kept.map(e => e.claim.claimId) })
        const result = await heartbeat(connection, o.table, {
          claims: kept.map(e => ({ id: e.claim.id, claimId: e.claim.claimId, lease: e.lease })),
        })
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
      if (failed) {
        failed = false
        o.onEvent({ type: "heartbeat-recovered" })
      }
      o.onSuccess()
    } catch (error) {
      succeeded = false
      failed = true
      o.onEvent({ type: "heartbeat-error", error })
      if (!(error instanceof QueryError) && connection) {
        const lost = connection
        connection = null
        await releaseConnection(lost)
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
        if (connection) {
          const held = connection
          connection = null
          await releaseConnection(held)
        }
      })()
      return stopping
    },
  }
}
