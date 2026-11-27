import { param, sql } from "kern-orm"

import { hasOccurrence } from "../../schedule/cron"
import { toScheduleScopes, type WorkerSchedule } from "../../schedule/schedule"
import { enqueueInScopes, enqueueItems } from "../../sql/enqueue"
import type { JobTable } from "../../sql/job-table"
import { toStatementLabel } from "../../sql/statement"
import { getTaskEnqueueParams } from "../../task/options"

import type { WorkerDb } from "./db"
import * as t from "./types"

/** Options of `evaluateSchedule`. */
export interface EvaluateOptions {
  /** Database time of the check instead of the start of its first transaction, for tests. */
  now?: Date | undefined
  /**
   * Whether the check may still write, asked before the first write of each of its transactions, and before and after the call of the function of the scopes; once a worker gives up its stop, the check neither calls the function nor writes in a transaction that has not begun to write, and takes no connection once the function has returned.
   */
  proceed?: (() => boolean) | undefined
}

/** Scopes of the one job of a schedule of a task that is not scoped: the empty scope. */
const unscoped: readonly string[] = [""]

/**
 * Checks one schedule under its row lock and advances its cursor in the same transaction as any enqueue or merge into a pending job, of every scope of the schedule.
 * For scopes that a function returns, a check that finds an enqueue due ends its transaction, calls the function, and checks again under the lock as of the time of the first transaction, so that the function runs while the check holds neither a connection nor the lock.
 */
export async function evaluateSchedule(
  db: WorkerDb,
  table: JobTable,
  schedule: WorkerSchedule,
  { now, proceed }: EvaluateOptions = {},
): Promise<void> {
  const { scopes } = schedule
  const time = now?.getTime()
  if (typeof scopes !== "function") {
    await checkUnderLock(db, table, schedule, {
      time,
      scopes: scopes ?? unscoped,
      wait: false,
      proceed,
    })
    return
  }
  const due = await checkUnderLock(db, table, schedule, {
    time,
    scopes: undefined,
    wait: false,
    proceed,
  })
  if (due === undefined || (proceed && !proceed())) {
    return
  }
  const list = toScheduleScopes(schedule.id, await scopes())
  // A stop that gave up while the function ran takes no connection for the transaction, which the application may have closed the pool of.
  if (proceed && !proceed()) {
    return
  }
  // The transaction decides by the cursor that it locks, which another worker may have advanced meanwhile, but as of the time of the first one: an occurrence that was due then stays due however long the function took, and a later minute, which this check never asked the scopes for, waits for the next check.
  // It waits for a worker that holds the row rather than skips it: that worker may check as of an earlier time and leave this occurrence due, which a later check may find past its catch-up window.
  await checkUnderLock(db, table, schedule, { time: due, scopes: list, wait: true, proceed })
}

/** Options of `checkUnderLock`. */
interface CheckOptions {
  /** Time of the check in milliseconds since the epoch instead of the start of the transaction. */
  time: number | undefined
  /** Scopes of the jobs that an occurrence enqueues, distinct and in the order of their enqueues, or `undefined` for a check that only finds whether an enqueue is due. */
  scopes: readonly string[] | undefined
  /** Whether to wait for the lock of the row, as long as the lock timeout of the transaction lets it, rather than skip a schedule whose row another transaction holds. */
  wait: boolean
  proceed: (() => boolean) | undefined
}

/**
 * Registers a schedule, locks its row, and in the same transaction enqueues in every scope what its cursor leaves due and advances the cursor.
 * Without scopes, it neither enqueues nor advances the cursor when an enqueue is due, and resolves to the time of the check in milliseconds since the epoch, for a check as of that time with the scopes; it keeps the registration of a schedule that had no row, so that a failure to get the scopes leaves the occurrence to the next check.
 * It resolves to `undefined` otherwise.
 */
async function checkUnderLock(
  db: WorkerDb,
  table: JobTable,
  schedule: WorkerSchedule,
  { time, scopes, wait, proceed }: CheckOptions,
): Promise<number | undefined> {
  // Both statements use the same database instant even when registration waits for another replica.
  const clock =
    time === undefined
      ? sql(t.epochMilliseconds)`transaction_timestamp()`
      : param(t.epochMilliseconds, new Date(time))
  return await db.$transaction(
    async () => {
      // The connection of a busy pool and the answer to the settings of the transaction can arrive after the timeout of stop, which no lock timeout bounds; the check then writes nothing.
      if (proceed && !proceed()) {
        return undefined
      }
      await db.schedule
        .insert({
          id: schedule.id,
          evaluatedThrough: sql`(date_trunc('minute', ${clock} at time zone 'UTC') at time zone 'UTC') - interval '1 minute'`,
        })
        .onConflict("id")
        .doNothing()
        .comment(toStatementLabel("schedule:register"))
      // READ COMMITTED sees a concurrently registered row after the insert waited for its transaction, so the lock is a statement of its own.
      const row = await db.schedule
        .find(schedule.id)
        .select(s => ({ cursor: s.evaluatedThrough, evaluated: clock }))
        .forUpdate({ skipLocked: !wait })
        .optional()
        .comment(toStatementLabel("schedule:lock"))
      // A replica holding this row owns the decision for now, unless the check waited for it.
      if (!row) {
        return undefined
      }
      // A given time is used as it is, so that the check decides by the number that the first transaction decided by, which the time of the database would only approximate.
      const at = time ?? row.evaluated
      const minute = Math.floor(at / 60000) * 60000
      const cursor = row.cursor
      if (minute <= cursor) {
        return undefined
      }
      if (schedule.enabled) {
        // The current minute stays eligible even when its start is older than the catch-up window.
        const earliest = Math.max(cursor + 60000, Math.min(minute, at - schedule.within * 1000))
        if (hasOccurrence(schedule.calendar, earliest, minute)) {
          if (!scopes) {
            return at
          }
          // No scopes enqueue nothing, so they run no function of the options of the task either.
          if (scopes.length > 0) {
            const params = getTaskEnqueueParams(schedule.task, schedule.payload, schedule.enqueue)
            const { maxItems, ...enqueueParams } = { ...params, now: new Date(at) }
            if (maxItems === undefined) {
              await enqueueInScopes(db.$database, table, enqueueParams, scopes)
            } else {
              // The payload of a schedule of a batch task is its array of items, and its job has no scope, since a schedule takes no scoped batch task.
              await enqueueItems(db.$database, table, { ...enqueueParams, maxItems })
            }
          }
        }
      }
      await db.schedule
        .find(schedule.id)
        .comment(toStatementLabel("schedule:advance"))
        .update({ evaluatedThrough: new Date(minute) })
      return undefined
    },
    // The timeout bounds every lock wait of the transaction, such as registration behind an uncommitted insert and an enqueue behind a producer holding its pending key.
    { isolation: "read committed", lockTimeout: 1000 },
  )
}
