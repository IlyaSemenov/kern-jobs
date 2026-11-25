// Fifth migration of kern-jobs: the SQL function `enqueue`, which enqueues a job from SQL, such as from a migration of the application, by the upsert of `sql/enqueue.ts`.
// The body of `enqueue` is a SQL-standard body, which PostgreSQL parses at its creation and binds to the table of the search path of the migration, so a call depends neither on the search path of its session nor on the name of the schema.
// A SQL-standard body has no RAISE statement, so `check_enqueue`, in PL/pgSQL, checks the arguments; it names no object of the schema, and its own search path, `pg_catalog, pg_temp`, gives the operators of its comparisons whatever the search path of the caller.

import { createFunction, defineMigration } from "kern-migrations"

export default defineMigration([
  createFunction(
    "check_enqueue",
    {
      task: "text",
      scope: "text",
      key_mode: "text",
      run_at: "timestamptz",
      delay: "float8",
      max_wait: "float8",
      max_attempts: "int",
    },
    `returns void language plpgsql set search_path = pg_catalog, pg_temp as $$
begin
  if task is null then
    raise exception 'task must not be null' using errcode = 'null_value_not_allowed';
  end if;
  -- The suffix names the failure task of a task, whose jobs a terminal failure inserts with a payload of its own, so defineTask rejects such a name too.
  if task like '%:failure' then
    raise exception 'The name of a task must not end with :failure, which names the failure task of a task, got %', task
      using errcode = 'invalid_parameter_value';
  end if;
  -- An unscoped job takes the omitted scope, so that a scope that a query fails to find, such as current_schema() of an empty search path, does not enqueue an unscoped job.
  if scope is null then
    raise exception 'scope must not be null: omit it for an unscoped job' using errcode = 'null_value_not_allowed';
  end if;
  if key_mode is not null and key_mode not in ('replace', 'keep', 'debounce') then
    raise exception 'key_mode must be replace, keep, or debounce, got %', key_mode using errcode = 'invalid_parameter_value';
  end if;
  if delay is not null and run_at is not null then
    raise exception 'Pass either delay or run_at, not both' using errcode = 'invalid_parameter_value';
  end if;
  if key_mode = 'debounce' and (delay is null or max_wait is null or run_at is not null) then
    raise exception 'key_mode debounce takes delay and max_wait, and no run_at' using errcode = 'invalid_parameter_value';
  end if;
  -- NaN is above every number in PostgreSQL, so the upper bound rejects it.
  if not (delay >= 0 and delay <= 1e12) then
    raise exception 'delay must be a number of seconds from 0 to 1e12, got %', delay using errcode = 'invalid_parameter_value';
  end if;
  -- Outside debounce, max_wait is ignored, as in the jobs API.
  if key_mode = 'debounce' and not (max_wait >= 0 and max_wait <= 1e12) then
    raise exception 'max_wait must be a number of seconds from 0 to 1e12, got %', max_wait using errcode = 'invalid_parameter_value';
  end if;
  -- The range of runAt of the jobs API, which also rejects infinity.
  if not (run_at >= '0001-01-01 00:00:00+00' and run_at < '10000-01-01 00:00:00+00') then
    raise exception 'run_at must be a time in the years 1 to 9999, got %', run_at using errcode = 'invalid_parameter_value';
  end if;
  if max_attempts < 1 then
    raise exception 'max_attempts must be an integer from 1 to 2147483647, got %', max_attempts using errcode = 'invalid_parameter_value';
  end if;
end
$$`,
  ),
  // A copy of the statement of `insertOne` and `mergeIntoPendingJob` of `sql/enqueue.ts` at this migration, whose test compares the two; an argument that is NULL takes its default, except `task` and `scope`, which `check_enqueue` rejects.
  // Arguments are qualified by the name of the function, since a column of the statement of the same name would take their place.
  createFunction(
    "enqueue",
    {
      task: "text",
      payload: { type: "jsonb", default: "null" },
      scope: { type: "text", default: "''" },
      key: { type: "text", default: "null" },
      queue: { type: "text", default: "null" },
      key_mode: { type: "text", default: "null" },
      run_at: { type: "timestamptz", default: "null" },
      delay: { type: "float8", default: "null" },
      max_wait: { type: "float8", default: "null" },
      max_attempts: { type: "int", default: "null" },
      reset_attempts: { type: "boolean", default: "null" },
      id: { mode: "out", type: "bigint" },
      created: { mode: "out", type: "boolean" },
    },
    `language sql
begin atomic
  select check_enqueue(enqueue.task, enqueue.scope, enqueue.key_mode, enqueue.run_at, enqueue.delay, enqueue.max_wait, enqueue.max_attempts);
  insert into job as j (task, scope, queue, key, key_mode, payload, run_at, window_started_at, created_at, max_attempts)
  select enqueue.task, enqueue.scope, enqueue.queue, enqueue.key, n.mode, enqueue.payload,
         case when n.mode = 'debounce' then n.t + make_interval(secs => least(enqueue.delay, enqueue.max_wait))
              else coalesce(enqueue.run_at, n.t + make_interval(secs => coalesce(enqueue.delay, 0))) end,
         n.t, n.t, coalesce(enqueue.max_attempts, 25)
  from (select statement_timestamp() as t, coalesce(enqueue.key_mode, 'replace') as mode) n
  on conflict (scope, task, key) where state = 'pending' and key is not null
  do update set
    key_mode     = excluded.key_mode,
    payload      = case when excluded.key_mode = 'keep' then j.payload else excluded.payload end,
    queue        = case when excluded.key_mode = 'keep' then j.queue else excluded.queue end,
    max_attempts = case when excluded.key_mode = 'keep' then j.max_attempts else excluded.max_attempts end,
    attempts     = case when excluded.key_mode <> 'keep' and coalesce(enqueue.reset_attempts, false) then 0 else j.attempts end,
    window_started_at = case when excluded.key_mode = 'debounce' and j.key_mode <> 'debounce' then excluded.window_started_at
                             else j.window_started_at end,
    run_at = case
      when excluded.key_mode = 'keep' then j.run_at
      when excluded.key_mode = 'replace' or j.key_mode <> 'debounce' then greatest(j.not_before, excluded.run_at)
      else greatest(j.not_before, j.run_at,
                    least(excluded.window_started_at + make_interval(secs => enqueue.delay),
                          j.window_started_at + make_interval(secs => enqueue.max_wait)))
    end
  -- xmax = 0 tells an inserted row from an updated one.
  returning j.id, j.xmax = 0;
end`,
  ),
])
