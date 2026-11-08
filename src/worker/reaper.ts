// Reaper loop of a worker: returns expired jobs in batches and survives connection errors.
import { reap } from "../core/lease"
import type { JobTable } from "../core/schema"
import type { StatementTarget } from "../core/statement"

import type { WorkerEvent } from "./events"
import { type Loop, startLoop } from "./loop"

/** Options of the reaper loop. */
export interface ReaperOptions {
  /** Seconds between rounds. */
  interval: number
  /** Jobs per batch. */
  limit: number
  onEvent(event: WorkerEvent): void
}

/**
 * Starts the reaper loop on a pool.
 * A failed round is reported and the next one runs after the interval: the pool replaces a lost connection, so the reaper resumes once the server is reachable again.
 * A full batch starts the next round at once.
 */
export function startReaper(db: StatementTarget, table: JobTable, o: ReaperOptions): Loop {
  return startLoop(
    async () => {
      const jobs = await reap(db, table, { limit: o.limit })
      if (jobs.length > 0) o.onEvent({ type: "reap", jobs })
      if (jobs.length === o.limit) return 0
    },
    { interval: o.interval, onError: error => o.onEvent({ type: "reap-error", error }) },
  )
}
