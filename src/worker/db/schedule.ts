import { param, sql } from "kern-orm"
import * as t from "kern-orm/types"

import { hasOccurrence } from "../../schedule/cron"
import type { WorkerSchedule } from "../../schedule/schedule"
import { enqueue } from "../../sql/enqueue"
import type { JobTable } from "../../sql/job-table"
import { getTaskEnqueueParams } from "../../task/options"

import type { WorkerDb } from "./db"

/** Options of `evaluateSchedule`. */
export interface EvaluateOptions {
  /** Database time of the check instead of the start of its transaction, for tests. */
  now?: Date | undefined
  /** Whether the check may still write, asked before its first write; a worker that gave up its stop writes nothing. */
  proceed?: (() => boolean) | undefined
}

/** Checks one schedule under its row lock and advances its cursor in the same transaction as any enqueue or merge into a pending job. */
export async function evaluateSchedule(
  db: WorkerDb,
  table: JobTable,
  schedule: WorkerSchedule,
  { now, proceed }: EvaluateOptions = {},
): Promise<void> {
  // Both statements use the same database instant even when registration waits for another replica.
  const clock = now ? param(t.timestamptz, now) : sql(t.timestamptz)`transaction_timestamp()`
  await db.$transaction(
    async () => {
      // The connection of a busy pool and the answer to the settings of the transaction can arrive after the timeout of stop, which no lock timeout bounds; the check then writes nothing.
      if (proceed && !proceed()) {
        return
      }
      await db.schedule
        .insert({
          id: schedule.id,
          evaluatedThrough: sql`(date_trunc('minute', ${clock} at time zone 'UTC') at time zone 'UTC') - interval '1 minute'`,
        })
        .onConflict("id")
        .doNothing()
      // READ COMMITTED sees a concurrently registered row after the insert waited for its transaction, so the lock is a statement of its own.
      // Epoch seconds keep the reading independent of the DateStyle of the pool, which the decoding of timestamptz by kern-orm requires to be ISO.
      const row = await db.schedule
        .find(schedule.id)
        .select(s => ({
          cursor: sql(t.numeric)`extract(epoch from ${s.evaluatedThrough})`,
          evaluated: sql(t.numeric)`extract(epoch from ${clock})`,
        }))
        .forUpdate({ skipLocked: true })
        .optional()
      // A replica holding this row owns the decision for now.
      if (!row) {
        return
      }
      const time = Number(row.evaluated) * 1000
      const minute = Math.floor(time / 60000) * 60000
      const cursor = Number(row.cursor) * 1000
      if (minute <= cursor) {
        return
      }
      if (schedule.enabled) {
        // The current minute stays eligible even when its start is older than the catch-up window.
        const earliest = Math.max(cursor + 60000, Math.min(minute, time - schedule.within * 1000))
        if (hasOccurrence(schedule.calendar, earliest, minute)) {
          const params = getTaskEnqueueParams(schedule.task, schedule.payload, schedule.enqueue)
          await enqueue(db.$database, table, { ...params, now: new Date(time) })
        }
      }
      await db.schedule.find(schedule.id).update({ evaluatedThrough: new Date(minute) })
    },
    // The timeout bounds every lock wait of the transaction, such as registration behind an uncommitted insert and an enqueue behind a producer holding its pending key.
    { isolation: "read committed", lockTimeout: 1000 },
  )
}
