// Enqueue: one upsert by the partial unique index of pending keys, of one job or of many.
import { NOW } from "./db"
import { encodePayload, type KeyMode } from "./job"
import type { JobTable } from "./schema"
import { queryValues, type ValuesTarget } from "./statement"

/** Attempts of a job whose enqueue gives no `maxAttempts`. */
export const DEFAULT_MAX_ATTEMPTS = 25

/** Parameters of an enqueue. */
export interface EnqueueParams {
  task: string
  scope?: string
  key?: string | null
  queue?: string | null
  /** Payload as JSON; an absent payload is stored as SQL NULL, which reads back as `undefined`. */
  payload?: unknown
  /** Key mode; `replace` when omitted. */
  mode?: KeyMode | undefined
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

/** Payload and options of one job of `enqueueMany`, whose task, scope, and time the jobs share. */
export type EnqueueJob = Omit<EnqueueParams, "task" | "scope" | "now">

/** Jobs of one task and scope that `enqueueMany` enqueues with one statement. */
export interface EnqueueBatch {
  task: string
  scope?: string
  jobs: readonly EnqueueJob[]
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
  const [result] = await prepareEnqueueMany(table, {
    task: o.task,
    scope: o.scope,
    jobs: [o],
    now: o.now,
  })(db)
  return result!
}

/**
 * Enqueues many jobs of one task and scope with one statement, as `prepareEnqueueMany` describes, and resolves to the result of each job in the order of the jobs.
 */
export async function enqueueMany(
  db: ValuesTarget,
  table: JobTable,
  batch: EnqueueBatch,
): Promise<EnqueueResult[]> {
  return prepareEnqueueMany(table, batch)(db)
}

/**
 * Prepares an enqueue of many jobs of one task and scope with one `INSERT … ON CONFLICT DO UPDATE` statement, which merges each job with a key into the pending job of its key by its mode, and returns the function that runs it on a target and resolves to the result of each job in the order of the jobs; it runs no statement for no jobs.
 * It serializes every payload before it returns, so that a payload that JSON cannot serialize throws before a target is needed.
 * The jobs must have distinct keys, since one statement cannot update a row twice: PostgreSQL rejects such a batch with `21000`.
 * The new rows get ids in the order of the jobs, and the statement locks the keys in that order.
 */
export function prepareEnqueueMany(
  table: JobTable,
  batch: EnqueueBatch,
): (db: ValuesTarget) => Promise<EnqueueResult[]> {
  const { task, scope = "", now = null } = batch
  const rows = batch.jobs.map(toEnqueueRow)
  if (rows.length === 0) return async () => []
  // One job takes a statement of its own parameters, which costs less than parsing an array.
  if (rows.length === 1) return async db => [await insertOne(db, table, task, scope, rows[0]!, now)]
  const conflicts = toConflictParams(rows)
  return async db =>
    toEnqueueResults(
      rows,
      await insertMany(
        db,
        table,
        task,
        scope,
        JSON.stringify(rows),
        JSON.stringify(conflicts),
        now,
      ),
    )
}

/** Values of the insert of a job, as `enqueue` and the JSON array of `enqueueMany` send them, with a payload as its JSON text, absent for SQL NULL. */
interface EnqueueRow {
  queue: string | null
  key: string | null
  mode: KeyMode
  payload: string | undefined
  run_at: string | null
  delay: number
  max_wait: number | null
  max_attempts: number
  reset_attempts: boolean
}

/** Returns the values of the insert of a job, and throws for a debounce without `maxWait` and the error of `encodePayload`. */
function toEnqueueRow(o: EnqueueJob): EnqueueRow {
  const mode = o.mode ?? "replace"
  if (mode === "debounce" && o.maxWait == null) throw new Error("maxWait required in debounce mode")
  return {
    queue: o.queue ?? null,
    key: o.key ?? null,
    mode,
    // The payload is serialized as a value of its own, since `toJSON` of a payload nested in the row would get another key.
    payload: encodePayload(o.payload),
    run_at: o.runAt?.toISOString() ?? null,
    delay: o.delay ?? 0,
    max_wait: o.maxWait ?? null,
    max_attempts: o.maxAttempts ?? DEFAULT_MAX_ATTEMPTS,
    reset_attempts: o.resetAttempts ?? false,
  }
}

/** SQL of the values of a job that the insert and the update of a conflict read. */
interface JobSql {
  mode: string
  runAt: string
  delay: string
  maxWait: string
  resetAttempts: string
}

/** SQL of the `run_at` of an inserted row: the time of the statement plus `least(delay, maxWait)` in `debounce` mode, otherwise `runAt` or the time plus `delay`. */
function insertedRunAt(job: Omit<JobSql, "resetAttempts">, now: string): string {
  return `case when ${job.mode} = 'debounce' then ${now} + make_interval(secs => least(${job.delay}, ${job.maxWait}))
               else coalesce(${job.runAt}, ${now} + make_interval(secs => ${job.delay})) end`
}

/** SQL of the `ON CONFLICT` clause that merges a job into the pending job of its key by the mode of the job, which `excluded.key_mode` gives. */
function mergeIntoPendingJob(job: Pick<JobSql, "delay" | "maxWait" | "resetAttempts">): string {
  // `keep` still runs DO UPDATE: it locks the pending row until the producer commits, so no worker starts the job before the event's transaction commits.
  // DO NOTHING would neither lock the row nor wait for the event.
  // A debounce event demands `least(now + delay, window_started_at + maxWait)` by its own settings, and `run_at` is the greatest demand of the window and `not_before`, so it never moves backwards within the window, whatever the settings and the commit order of its events.
  // A debounce event over a row of another key mode opens a new window, as the insert does, rather than reading the window and the run time of an enqueue in another mode.
  return `on conflict (scope, task, key) where state = 'pending' and key is not null
     do update set
       key_mode     = excluded.key_mode,
       payload      = case when excluded.key_mode = 'keep' then j.payload else excluded.payload end,
       queue        = case when excluded.key_mode = 'keep' then j.queue else excluded.queue end,
       max_attempts = case when excluded.key_mode = 'keep' then j.max_attempts else excluded.max_attempts end,
       attempts     = case when excluded.key_mode <> 'keep' and ${job.resetAttempts} then 0 else j.attempts end,
       window_started_at = case when excluded.key_mode = 'debounce' and j.key_mode <> 'debounce' then excluded.window_started_at
                                else j.window_started_at end,
       run_at = case
         when excluded.key_mode = 'keep' then j.run_at
         when excluded.key_mode = 'replace' or j.key_mode <> 'debounce' then greatest(j.not_before, excluded.run_at)
         else greatest(j.not_before, j.run_at,
                       least(excluded.window_started_at + make_interval(secs => ${job.delay}),
                             j.window_started_at + make_interval(secs => ${job.maxWait})))
       end`
}

/** Inserts one job, or merges it into the pending job of its key, by a statement of its own parameters. */
async function insertOne(
  db: ValuesTarget,
  table: JobTable,
  task: string,
  scope: string,
  row: EnqueueRow,
  now: Date | null,
): Promise<EnqueueResult> {
  const job: JobSql = {
    mode: "$7::text",
    runAt: "$8::text::timestamptz",
    delay: "$9::text::float8",
    maxWait: "$10::text::float8",
    resetAttempts: "$12::text::boolean",
  }
  const values = await queryValues(
    db,
    // `xmax = 0` tells an inserted row from an updated one.
    // The statement runs on connections of the application too, whose client may serialize a parameter by its type and rename the columns of the result, so every parameter is cast from text, and the result is one text column.
    `insert into ${table.name} as j (task, scope, queue, key, key_mode, payload, run_at, window_started_at, created_at, max_attempts)
     select $2::text, $3::text, $4::text, $5::text, $7::text, $6::text::jsonb, ${insertedRunAt(job, "n.t")}, n.t, n.t, $11::text::int
     from (select ${NOW} as t) n
     ${mergeIntoPendingJob(job)}
     returning json_build_array(j.id::text, j.xmax = 0)::text`,
    [
      now,
      task,
      scope,
      row.queue,
      row.key,
      row.payload,
      row.mode,
      row.run_at,
      row.delay,
      row.max_wait,
      row.max_attempts,
      row.reset_attempts,
    ],
  )
  const [id, created] = JSON.parse(values[0]![0]!) as [string, boolean]
  return { id, created }
}

/** Row that the statement of many jobs returns: the id and the key of the row, and whether the statement inserted it. */
type EnqueuedRow = [id: string, key: string | null, created: boolean]

/**
 * Inserts the jobs of a JSON array of `EnqueueRow` of one task and scope, or merges each job with a key into the pending job of its key, by one statement, and returns a row of each job in no order.
 */
async function insertMany(
  db: ValuesTarget,
  table: JobTable,
  task: string,
  scope: string,
  rows: string,
  conflicts: string,
  now: Date | null,
): Promise<EnqueuedRow[]> {
  // A conflict reads the values of its job that are not columns of `excluded` from the object of the jobs with a key by its key; the subquery parses the object once per statement, rather than at each conflict.
  const conflictValue = (index: number) =>
    `((select params from conflict) #>> array[excluded.key, '${index}'])`
  const job = { mode: "i.mode", runAt: "i.run_at", delay: "i.delay", maxWait: "i.max_wait" }
  const values = await queryValues(
    db,
    // The rows are inserted in the order of the jobs, so the identity of the table gives the new rows ids in that order, and a queue starts them in that order at an equal `run_at`.
    // The keys are locked in that order too, so transactions that enqueue overlapping keys in different orders can deadlock, as single enqueues in a transaction can.
    // Each row of the result is one text column, as that of `insertOne`.
    `with conflict as (select $5::text::jsonb as params)
     insert into ${table.name} as j (task, scope, queue, key, key_mode, payload, run_at, window_started_at, created_at, max_attempts)
     select $2::text, $3::text, i.queue, i.key, i.mode, i.payload::jsonb, ${insertedRunAt(job, "n.t")}, n.t, n.t, i.max_attempts
     from (select ${NOW} as t) n,
          (
            select e.item->>'queue' as queue, e.item->>'key' as key, e.item->>'mode' as mode, e.item->>'payload' as payload,
                   (e.item->>'run_at')::timestamptz as run_at, (e.item->>'delay')::float8 as delay,
                   (e.item->>'max_wait')::float8 as max_wait, (e.item->>'max_attempts')::int as max_attempts, e.ord
            from jsonb_array_elements($4::text::jsonb) with ordinality e(item, ord)
          ) i
     order by i.ord
     ${mergeIntoPendingJob({
       delay: `${conflictValue(0)}::float8`,
       maxWait: `${conflictValue(1)}::float8`,
       resetAttempts: `${conflictValue(2)}::boolean`,
     })}
     returning json_build_array(j.id::text, j.key, j.xmax = 0)::text`,
    [now, task, scope, rows, conflicts],
  )
  return values.map(([text]) => JSON.parse(text!) as EnqueuedRow)
}

/** Object of `delay`, `maxWait`, and `resetAttempts` of the jobs with a key by their keys, which the update of a conflict reads. */
type ConflictParams = Record<
  string,
  [delay: number, maxWait: number | null, resetAttempts: boolean]
>

/** Returns the values of the conflicts of the jobs with a key by their keys. */
function toConflictParams(rows: readonly EnqueueRow[]): ConflictParams {
  // An object without a prototype keeps any key as an own property, also `__proto__`, which an assignment to an ordinary object would take as its prototype.
  const params: ConflictParams = Object.create(null)
  for (const row of rows) {
    if (row.key !== null) params[row.key] = [row.delay, row.max_wait, row.reset_attempts]
  }
  return params
}

/**
 * Returns the results of the jobs in their order from the rows of the statement: a job with a key takes the row of its key, and the jobs without a key take the new rows without a key in the ascending order of their ids, which the statement inserted in the order of the jobs.
 */
function toEnqueueResults(
  jobs: readonly EnqueueRow[],
  rows: readonly EnqueuedRow[],
): EnqueueResult[] {
  const keyed = new Map<string, EnqueueResult>()
  const unkeyed: EnqueueResult[] = []
  for (const [id, key, created] of rows) {
    if (key === null) unkeyed.push({ id, created })
    else keyed.set(key, { id, created })
  }
  unkeyed.sort((a, b) => (BigInt(a.id) < BigInt(b.id) ? -1 : 1))
  let next = 0
  return jobs.map(row => (row.key === null ? unkeyed[next++]! : keyed.get(row.key)!))
}
