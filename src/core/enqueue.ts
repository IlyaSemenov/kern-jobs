// Enqueue: one upsert by the partial unique index of pending keys.
import { NOW } from "./db"
import type { KeyMode } from "./job"
import type { JobTable } from "./schema"
import { queryValues, type ValuesTarget } from "./statement"

/** Parameters of an enqueue. */
export interface EnqueueParams {
  task: string
  scope?: string
  key?: string | null
  queue?: string | null
  /** Payload as JSON; an absent payload is stored as SQL NULL, which reads back as `undefined`. */
  payload?: unknown
  mode?: KeyMode
  /** Run time in `replace` and `keep` modes, which takes precedence over `delay`. */
  runAt?: Date | null
  /**
   * Seconds from the time of the statement to the run time: in `replace` and `keep` modes without `runAt`, and as the debounce delay in `debounce` mode.
   */
  delay?: number | null
  /**
   * Seconds from the start of the debounce window to the latest run time that this event demands, required in `debounce` mode.
   */
  maxWait?: number | null
  maxAttempts?: number
  /** Resets the attempts of an existing pending job; ignored in `keep` mode. */
  resetAttempts?: boolean
  /** Time of the statement; `statement_timestamp()` when omitted. */
  now?: Date | null
}

/** Result of an enqueue. */
export interface EnqueueResult {
  /** Id of the pending job that the enqueue inserted or updated. */
  id: string
  /** Whether the enqueue inserted the job rather than merged into the pending job of its key. */
  created: boolean
}

/**
 * Enqueues a job with one `INSERT … ON CONFLICT DO UPDATE` statement by the partial unique index of pending keys.
 * Returns the id of the job and whether the statement inserted it.
 */
export async function enqueue(
  db: ValuesTarget,
  table: JobTable,
  o: EnqueueParams,
): Promise<EnqueueResult> {
  const mode = o.mode ?? "replace"
  if (mode === "debounce" && o.maxWait == null) throw new Error("maxWait required in debounce mode")
  const rows = await queryValues(
    db,
    // `keep` still runs DO UPDATE: it locks the pending row until the producer commits, so no worker starts the job before the event's transaction commits.
    // DO NOTHING would neither lock the row nor wait for the event.
    // A debounce event demands `least(now + delay, window_started_at + maxWait)` by its own settings, and `run_at` is the greatest demand of the window and `not_before`, so it never moves backwards within the window, whatever the settings and the commit order of its events.
    // A debounce event over a row of another key mode opens a new window, as the insert does, rather than reading the window and the run time of an enqueue in another mode.
    // `xmax = 0` tells an inserted row from an updated one.
    // The statement runs on connections of the application too, whose client may serialize a parameter by its type and rename the columns of the result, so every parameter is cast from text, and the result is one text column.
    `insert into ${table.name} as j (task, scope, queue, key, key_mode, payload, run_at, window_started_at, created_at, max_attempts)
     select $2::text, $3::text, $4::text, $5::text, $7::text, $6::text::jsonb,
            case when $7::text = 'debounce' then n.t + make_interval(secs => least($9::text::float8, $10::text::float8))
                 else coalesce($8::text::timestamptz, n.t + make_interval(secs => $9::text::float8)) end,
            n.t, n.t, $11::text::int
     from (select ${NOW} as t) n
     on conflict (scope, task, key) where state = 'pending' and key is not null
     do update set
       key_mode     = excluded.key_mode,
       payload      = case when $7::text = 'keep' then j.payload else excluded.payload end,
       queue        = case when $7::text = 'keep' then j.queue else excluded.queue end,
       max_attempts = case when $7::text = 'keep' then j.max_attempts else excluded.max_attempts end,
       attempts     = case when $7::text <> 'keep' and $12::text::boolean then 0 else j.attempts end,
       window_started_at = case when $7::text = 'debounce' and j.key_mode <> 'debounce' then excluded.window_started_at
                                else j.window_started_at end,
       run_at = case
         when $7::text = 'keep' then j.run_at
         when $7::text = 'replace' or j.key_mode <> 'debounce' then greatest(j.not_before, excluded.run_at)
         else greatest(j.not_before, j.run_at,
                       least(excluded.window_started_at + make_interval(secs => $9::text::float8),
                             j.window_started_at + make_interval(secs => $10::text::float8)))
       end
     returning json_build_array(j.id::text, j.xmax = 0)::text`,
    [
      o.now ?? null,
      o.task,
      o.scope ?? "",
      o.queue ?? null,
      o.key ?? null,
      o.payload === undefined ? null : JSON.stringify(o.payload),
      mode,
      o.runAt ?? null,
      o.delay ?? 0,
      o.maxWait ?? null,
      o.maxAttempts ?? 25,
      o.resetAttempts ?? false,
    ],
  )
  const [id, created] = JSON.parse(rows[0]![0]!) as [string, boolean]
  return { id, created }
}
