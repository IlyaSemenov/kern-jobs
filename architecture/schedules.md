# Schedules

A schedule enqueues an unscoped task on a UTC calendar.
Every worker whose manifest lists the schedule checks it.
A shared cursor records the minutes already checked, so workers cannot enqueue the same minute twice.

- The schedule ID identifies the schedule in the schema of kern-jobs and defaults to the task name.
  It must be [storable text](README.md#terms).
- `defineSchedule` returns the definition as given, with defaults filled.
  `toWorkerSchedule` checks the definition and compiles the calendar; `defineSchedule`, `defineManifest`, and `startWorker` each run it, since any object of the shape of a schedule passes the types, and a manifest rejects a reused schedule ID.
  For a task without `withOptions`, it checks the merged enqueue options.
  For a task with it, it checks the enqueue overrides without calling the function, for errors that no option of the task can correct; each enqueue checks the merged options.
  The evaluator takes only the compiled schedule, so the public type does not carry the representation of the calendar.
- The application must not mutate a schedule or its nested options after `startWorker`: the worker compiles the calendar at start but reads the payload and the enqueue overrides at each enqueue.
- Each enqueue uses the schedule's static payload, resolves the task options for that payload, and applies the schedule's enqueue overrides.
  Only an explicit task or schedule option gives the job a key.
- The handler may run on another worker in the same schema.

## Calendar

- A five-field string specifies minute, hour, day of month, month, and day of week, in that order.
- A structured calendar requires `minute`, including `"*"` for every minute.
  Each omitted field selects its whole domain.
- A field accepts a single value, a list, an inclusive range with `from` and `to`, or a step with `every` and optional bounds.
- Ranges run forwards without wrapping.
  Steps count from the lower bound of the range or the field minimum.
- Weekdays run from Sunday (0) to Saturday (6), with 7 as an alias for Sunday.
  The three-letter English names are also accepted.
- Both forms compile to the same five sorted sets of numbers, ignoring duplicates.
- If both day fields restrict their domains, either may match.
  If only one restricts its domain, it must match.
  A selection covering a field's whole domain is equivalent to a wildcard.
- A calendar must select an existing day: one whose weekday field selects every day and whose days of month exist in none of its months, such as February 30, is rejected when it is compiled; February 29 is accepted, and a restricted weekday field selects days by itself, as in `0 0 30 2 mon`.

## Check

After the schema check, a worker checks its schedules independently of handler slots and starts its next round 15 seconds after the previous round ends.
Each schedule check runs in its own transaction, using the database time at the start of that transaction for both evaluation and enqueue.
Let M be the UTC minute containing that instant.

1. The worker inserts the schedule row if absent, setting `evaluated_through` to one minute before M so that only M is eligible on first registration.
2. It locks the row with `FOR UPDATE SKIP LOCKED`.
   If another transaction holds the lock, it skips this schedule.
3. It returns if M is at or before `evaluated_through`, the last minute checked, inclusive.
4. For an enabled schedule, it enqueues one job if M matches or if a matching minute after the cursor is at most `catchUp.within` seconds old.
   Without `catchUp`, only M is eligible.
   One job covers all eligible minutes so that a reconciliation runs once after downtime ([decision](../decisions/schedules/one-job-per-recovery-window.md)).
5. It sets `evaluated_through` to M, including when the schedule is disabled or no minute matched.

- The transaction bounds each of its lock waits to one second, including registration and enqueue.
  An error before `COMMIT`, such as a lock timeout or a throwing `withOptions`, rolls it back, and the worker reports a schedule error and continues to its next schedule.
- A worker checks its schedules one at a time in every round.
  `stop` starts no further check and waits for the one in flight; a check that has not begun to write by the timeout of `stop` writes nothing, and one that has may still commit in the background.
- The cursor advance commits atomically with the enqueue, including a merge into an existing pending job.
  After rollback or an unknown commit outcome, the next check reads the durable cursor.
  Completing or deleting the job leaves the cursor in place ([decision](../decisions/schedules/occurrence-cursor.md)).
- A job's `delay` counts from the evaluation time, including on recovery, and `runAt` keeps its absolute meaning.

At 03:00:40, the 03:00 occurrence is the current minute M, so it is eligible even with a 30-second window if it has yet to be checked.
At 03:01:00, recovering that occurrence for an already registered schedule requires a window of at least 60 seconds.

## Configuration changes

- The row holds only the schedule ID and its inclusive cursor.
  The active calendar and catch-up window apply to unevaluated minutes, including a gap before a configuration change.
- Minutes checked while disabled remain processed after re-enabling.
- A schedule omitted from every manifest counts as down until a worker lists it again.
- Workers that list the same ID must use the same definition, including `enabled`, the payload, and the enqueue options ([decision](../decisions/schedules/one-definition-per-id.md)).
  If definitions differ, such as during a rolling deployment, the worker that first commits the cursor advance decides which minutes it covers and whether to enqueue, so an occurrence of only one definition can be lost.
  A changed definition under a new ID registers its own row, so neither definition advances the cursor of the other, and both enqueue until the workers of the old one stop; it starts at the minute of its first check, like any new ID.
  To change a definition under the same ID, the application must stop every worker that lists the ID before a worker with the new definition starts.

## Accepted limitations

- A lock timeout can delay the next successful check past the matching minute.
  Recovery then requires a catch-up window that still includes the occurrence.
- A rolled-back first registration leaves no row, so the next check registers the schedule again at its own minute, and catch-up recovers no occurrence before it.
- Rows for schedules removed from every manifest remain in the database.
- A slow check delays the later schedules of its round.
- Each worker sends a transaction per schedule every round, also for a minute that it has already seen checked.

## Open questions

- There is no API for removing the row of a retired schedule.
