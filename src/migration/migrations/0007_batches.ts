// Batch jobs: the columns `max_items` and `sealed`, the unique index of the jobs that take more input in place of that of every pending job with a key, and the SQL function `enqueue_items`, which appends the items of a batch task.
// `enqueue` and `retry` take the new index, and `enqueue` rejects a merge into a batch; `retry` returns a failed batch job, which its claim sealed, whatever the pending jobs of its key.

import {
  alterTable,
  createFunction,
  createIndex,
  defineMigration,
  dropFunction,
  dropIndex,
  replaceFunction,
} from "kern-migrations"

import { enqueueDefinition, enqueueParameters } from "./0005_enqueue_function"
import { retryDefinition, retryParameters } from "./0006_retry_function"

export default defineMigration([
  // The body of enqueue depends on every column of the table through its ON CONFLICT, so it is dropped before the columns are added, and a rollback creates it after they are dropped.
  dropFunction("enqueue", enqueueParameters, enqueueDefinition),
  // The capacity of a batch job, null for a job that is not a batch, and whether a batch job takes no more items.
  // NOT VALID skips the scan of the existing rows, which all pass, since their max_items is null and their sealed false; every later write is checked.
  alterTable("job", {
    addColumns: { max_items: "int", sealed: "boolean not null default false" },
    addConstraints: {
      job_max_items_check: "check (max_items >= 1) not valid",
      job_sealed_check: "check (not sealed or max_items is not null) not valid",
    },
  }),
  // A sealed batch job keeps its key but takes no more input, so several of a key may wait beside the job that does.
  dropIndex("job", ["scope", "task", "key"], {
    name: "job_pending_key_idx",
    unique: true,
    where: "state = 'pending' and key is not null",
  }),
  createIndex("job", ["scope", "task", "key"], {
    name: "job_open_key_idx",
    unique: true,
    where: "state = 'pending' and key is not null and not sealed",
  }),
  // Finds the sealed batch jobs of a key, which the unique index leaves out, for a delete of the pending jobs of a key.
  createIndex("job", ["scope", "task", "key"], {
    name: "job_sealed_key_idx",
    where: "state = 'pending' and sealed",
  }),
  // Raises when an enqueue would merge into a pending job of another kind: items into a job that is not a batch, or a job that is not a batch into a batch; returns the capacity of the pending job otherwise.
  // It names no object of the schema, as check_enqueue does.
  createFunction(
    "check_merge",
    { task: "text", key: "text", pending_max_items: "int", max_items: "int" },
    `returns int language plpgsql set search_path = pg_catalog, pg_temp as $$
begin
  if pending_max_items is null and max_items is not null then
    raise exception 'The pending job of key % of task % is not a batch, so items cannot append to it', quote_literal(key), task
      using errcode = 'object_not_in_prerequisite_state';
  end if;
  if pending_max_items is not null and max_items is null then
    raise exception 'The pending job of key % of task % is a batch, so a job that is not a batch cannot merge into it', quote_literal(key), task
      using errcode = 'object_not_in_prerequisite_state';
  end if;
  return pending_max_items;
end
$$`,
  ),
  // Checks the arguments of enqueue_items that check_enqueue does not.
  createFunction(
    "check_items",
    { key: "text", key_mode: "text", items: "jsonb", max_items: "int" },
    `returns void language plpgsql set search_path = pg_catalog, pg_temp as $$
begin
  if max_items is null or max_items < 1 then
    raise exception 'max_items must be an integer from 1 to 2147483647, got %', max_items using errcode = 'invalid_parameter_value';
  end if;
  if jsonb_typeof(items) is distinct from 'array' then
    raise exception 'items must be a JSON array' using errcode = 'invalid_parameter_value';
  end if;
  -- An empty array appends to no key, as in the jobs API.
  if key is null and jsonb_array_length(items) > 0 then
    raise exception 'A batch requires a key' using errcode = 'invalid_parameter_value';
  end if;
  -- keep would discard the items of every enqueue after the first.
  if key_mode = 'keep' then
    raise exception 'key_mode keep does not apply to a batch' using errcode = 'invalid_parameter_value';
  end if;
end
$$`,
  ),
  // Appends items to the open batch of their key and splits what does not fit into new jobs: full ones, sealed and due at once, and at most one partial one, which takes more items.
  // An open batch of another capacity, such as after a deploy that changes maxItems, is sealed with its items and run time, and the items go to new jobs.
  // The advisory lock serializes the batch enqueues of a key until the transaction ends, so the next statement, whose snapshot is new, sees the open batch of an enqueue that held the lock, and two enqueues never both insert one; claims, which take no such lock, skip a batch that the enqueue holds.
  // It returns a row of each job that took items, in the order of their items: the open batch first, if it takes items, then the new jobs; none for no items.
  createFunction(
    "enqueue_items",
    {
      task: "text",
      items: "jsonb",
      max_items: "int",
      scope: { type: "text", default: "''" },
      key: { type: "text", default: "null" },
      queue: { type: "text", default: "null" },
      key_mode: { type: "text", default: "null" },
      run_at: { type: "timestamptz", default: "null" },
      delay: { type: "float8", default: "null" },
      max_wait: { type: "float8", default: "null" },
      max_attempts: { type: "int", default: "null" },
      reset_attempts: { type: "boolean", default: "null" },
      now: { type: "timestamptz", default: "null" },
      id: { mode: "out", type: "bigint" },
      created: { mode: "out", type: "boolean" },
    },
    `returns setof record language sql
begin atomic
  select check_enqueue(enqueue_items.task, enqueue_items.scope, enqueue_items.key_mode, enqueue_items.run_at, enqueue_items.delay,
                       enqueue_items.max_wait, enqueue_items.max_attempts);
  select check_items(enqueue_items.key, enqueue_items.key_mode, enqueue_items.items, enqueue_items.max_items);
  -- The oid of the table, bound at the creation of the function, tells the installations of one database apart; JSON keeps a separator in a value from making two keys equal.
  select pg_advisory_xact_lock(hashtextextended(
           json_build_array('kern-jobs:batch', 'job'::regclass::oid, enqueue_items.scope, enqueue_items.task, enqueue_items.key)::text, 0))
  where jsonb_array_length(enqueue_items.items) > 0;
  with n as (
    select s.t, s.mode, jsonb_array_length(enqueue_items.items) as k,
           case when s.mode = 'debounce' then s.t + make_interval(secs => least(enqueue_items.delay, enqueue_items.max_wait))
                else coalesce(enqueue_items.run_at, s.t + make_interval(secs => coalesce(enqueue_items.delay, 0))) end as run_at
    from (select coalesce(enqueue_items.now, statement_timestamp()) as t, coalesce(enqueue_items.key_mode, 'replace') as mode) s
  ), o as (
    select j.id, j.task, j.key, j.max_items,
           case when jsonb_typeof(j.payload) = 'array' then jsonb_array_length(j.payload) else 0 end as len
    from job j, n
    where j.state = 'pending' and j.scope = enqueue_items.scope and j.task = enqueue_items.task and j.key = enqueue_items.key
      and not j.sealed and n.k > 0
    for update of j
  ), c as (
    -- check_merge raises for a pending job that is not a batch before anything is written.
    select o.id, check_merge(o.task, o.key, o.max_items, enqueue_items.max_items) as max_items, o.len,
           case when o.max_items = enqueue_items.max_items then least(greatest(enqueue_items.max_items - o.len, 0), n.k) else 0 end as fill
    from o, n
  ), resized as (
    -- An open batch of another capacity takes no more items.
    update job j set sealed = true
    from c
    where j.id = c.id and c.max_items <> enqueue_items.max_items
    returning j.id
  ), u as (
    update job j set
      payload = j.payload || coalesce((select jsonb_agg(e.v order by e.i) from jsonb_array_elements(enqueue_items.items) with ordinality e(v, i)
                                       where e.i <= c.fill), '[]'),
      sealed = c.len + c.fill >= c.max_items,
      key_mode = n.mode,
      queue = enqueue_items.queue,
      max_attempts = coalesce(enqueue_items.max_attempts, 25),
      attempts = case when coalesce(enqueue_items.reset_attempts, false) then 0 else j.attempts end,
      window_started_at = case when n.mode = 'debounce' and j.key_mode <> 'debounce' then n.t else j.window_started_at end,
      -- A full batch is due at once; a partial one takes the run time of a merge of its key mode.
      run_at = case
        when c.len + c.fill >= c.max_items then greatest(j.not_before, n.t)
        when n.mode = 'replace' or j.key_mode <> 'debounce' then greatest(j.not_before, n.run_at)
        else greatest(j.not_before, j.run_at,
                      least(n.t + make_interval(secs => enqueue_items.delay),
                            j.window_started_at + make_interval(secs => enqueue_items.max_wait)))
      end
    from c, n
    where j.id = c.id and c.fill > 0
    returning j.id
  ), r as materialized (
    -- Reading u and resized runs the updates before the insert, so a new partial batch never meets the open batch that they seal.
    select coalesce((select c.fill from c), 0) as fill, (select count(*) from u) + (select count(*) from resized) as updated
  ), chunks as (
    select (e.i - r.fill - 1) / enqueue_items.max_items as ord, jsonb_agg(e.v order by e.i) as payload, count(*) as size
    from r, jsonb_array_elements(enqueue_items.items) with ordinality e(v, i)
    where e.i > r.fill and r.updated >= 0
    group by 1
  ), ins as (
    insert into job as j (task, scope, queue, key, key_mode, payload, run_at, window_started_at, created_at, max_attempts,
                          max_items, sealed)
    select enqueue_items.task, enqueue_items.scope, enqueue_items.queue, enqueue_items.key, n.mode, ch.payload,
           case when ch.size = enqueue_items.max_items then n.t else n.run_at end, n.t, n.t,
           coalesce(enqueue_items.max_attempts, 25), enqueue_items.max_items, ch.size = enqueue_items.max_items
    from n, chunks ch
    -- The identity gives the new jobs ids in the order of their items.
    order by ch.ord
    returning j.id
  )
  select x.id, x.created from (
    select u.id, false as created, 0::bigint as ord from u
    union all
    select ins.id, true, ins.id from ins
  ) x
  order by x.ord;
end`,
  ),
  // A copy of the statement of `insertOne` and `mergeIntoPendingJob` of `sql/enqueue.ts` at this migration, whose test compares the two.
  createFunction(
    "enqueue",
    enqueueParameters,
    `language sql
begin atomic
  select check_enqueue(enqueue.task, enqueue.scope, enqueue.key_mode, enqueue.run_at, enqueue.delay, enqueue.max_wait, enqueue.max_attempts);
  insert into job as j (task, scope, queue, key, key_mode, payload, run_at, window_started_at, created_at, max_attempts)
  select enqueue.task, enqueue.scope, enqueue.queue, enqueue.key, n.mode, enqueue.payload,
         case when n.mode = 'debounce' then n.t + make_interval(secs => least(enqueue.delay, enqueue.max_wait))
              else coalesce(enqueue.run_at, n.t + make_interval(secs => coalesce(enqueue.delay, 0))) end,
         n.t, n.t, coalesce(enqueue.max_attempts, 25)
  from (select statement_timestamp() as t, coalesce(enqueue.key_mode, 'replace') as mode) n
  on conflict (scope, task, key) where state = 'pending' and key is not null and not sealed
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
    end,
    -- Raises for a pending batch of the key.
    max_items    = check_merge(j.task, j.key, j.max_items, null)
  -- xmax = 0 tells an inserted row from an updated one.
  returning j.id, j.xmax = 0;
end`,
  ),
  // A copy of the statement of `retryFailedJob` of `producer/administration.ts` at this migration, as in `0006_retry_function`.
  replaceFunction(
    "retry",
    retryParameters,
    retryDefinition,
    `returns setof record language sql
begin atomic
  with f as (
    select j.id, j.scope, j.task, j.key, j.max_items from job j
    where j.id = retry.id and j.state = 'failed'
    for update
  ), c as (
    select p.id from job p, f
    where f.max_items is null and p.state = 'pending' and not p.sealed and p.scope = f.scope and p.task = f.task and p.key = f.key
    for update of p
  ), u as (
    update job j set state = 'pending', attempts = 0, run_at = n.t, not_before = null, window_started_at = n.t,
      failed_at = null
    from f, (select statement_timestamp() as t) n
    where j.id = f.id and not exists (select from c)
    returning j.id
  )
  select exists (select from u), (select c.id from c) from f;
end`,
  ),
])
