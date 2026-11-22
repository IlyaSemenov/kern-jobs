import type { Database } from "kern-orm/db"

import { quoteIdentifier } from "../core/db"
import { enqueue } from "../core/enqueue"
import type { JobTable } from "../core/schema"
import { query } from "../core/statement"
import { getTaskEnqueueParams } from "../task/options"

import { hasOccurrence } from "./cron"
import type { WorkerSchedule } from "./schedule"

/** Options of `evaluateSchedule`. */
export interface EvaluateOptions {
  /** Database time of the check instead of the start of its transaction, for tests. */
  now?: Date | undefined
  /** Whether the check may still write, asked before its first write; a worker that gave up its stop writes nothing. */
  proceed?: (() => boolean) | undefined
}

/** Checks one schedule under its row lock and advances its cursor in the same transaction as any enqueue or merge into a pending job. */
export async function evaluateSchedule(
  database: Database,
  table: JobTable,
  schedule: WorkerSchedule,
  { now, proceed }: EvaluateOptions = {},
): Promise<void> {
  const name = `${quoteIdentifier(table.schemaName)}.schedule`
  // Both statements use the same database instant even when registration waits for another replica.
  const clock = "coalesce($1::text::timestamptz, transaction_timestamp())"
  await database.transaction(
    async () => {
      // The timeout bounds every lock wait of the transaction, such as registration behind an uncommitted insert and an enqueue behind a producer holding its pending key.
      await query(database, "set local lock_timeout = '1s'")
      // The connection of a busy pool and the answer to the setting can arrive after the timeout of stop, which no lock timeout bounds; the check then writes nothing.
      if (proceed && !proceed()) {
        return
      }
      await query(
        database,
        `insert into ${name} (id, evaluated_through)
      values ($2::text, (date_trunc('minute', ${clock} at time zone 'UTC') at time zone 'UTC') - interval '1 minute')
      on conflict (id) do nothing`,
        [now, schedule.id],
      )
      // READ COMMITTED sees a concurrently registered row after the insert waited for its transaction.
      const [row] = await query(
        database,
        `select extract(epoch from evaluated_through) as cursor, extract(epoch from ${clock}) as evaluated
      from ${name} where id = $2::text for update skip locked`,
        [now, schedule.id],
      )
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
          await enqueue(database, table, { ...params, now: new Date(time) })
        }
      }
      await query(
        database,
        `update ${name} set evaluated_through = $1::text::timestamptz where id = $2::text`,
        [new Date(minute), schedule.id],
      )
    },
    { isolation: "read committed" },
  )
}
