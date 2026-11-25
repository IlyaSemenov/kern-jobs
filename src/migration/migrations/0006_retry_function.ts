// The SQL function `retry`, which returns a failed job to pending from SQL by the statement of `retryFailedJob` of `producer/administration.ts`.
// Its body is a SQL-standard body, which PostgreSQL parses at its creation and binds to the table of the search path of the migration, as the body of `enqueue` is, so a call depends neither on the search path of its session nor on the name of the schema.

import { createFunction, defineMigration } from "kern-migrations"

export default defineMigration([
  // A copy of the statement of `retryFailedJob` at this migration, without a scope and with the time of the statement, whose test compares the two; it returns no row when no failed job has the id.
  // The argument is qualified by the name of the function, since the column `id` would take its place.
  createFunction(
    "retry",
    {
      id: "bigint",
      retried: { mode: "out", type: "boolean" },
      pending_id: { mode: "out", type: "bigint" },
    },
    `returns setof record language sql
begin atomic
  with f as (
    select j.id, j.scope, j.task, j.key from job j
    where j.id = retry.id and j.state = 'failed'
    for update
  ), c as (
    select p.id from job p, f
    where p.state = 'pending' and p.scope = f.scope and p.task = f.task and p.key = f.key
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
