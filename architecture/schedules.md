# Schedules

A schedule enqueues a task on a UTC calendar: one job of a task that is not scoped, or a job in each of the [scopes](#scopes) of the schedule for a scoped task.
Every worker whose manifest lists the schedule checks it.
A shared cursor records the minutes already checked, so workers cannot enqueue the same minute twice.

- The schedule ID identifies the schedule in the schema of kern-jobs and defaults to the task name.
  It must be [storable text](README.md#terms).
- `defineSchedule` returns the definition as given, with defaults filled.
  `toWorkerSchedule` checks the definition and compiles the calendar; `defineSchedule`, `defineManifest`, and `startWorker` each run it, since any object of the shape of a schedule passes the types, and a manifest rejects a reused schedule ID.
  For a task without `withOptions`, it checks the merged enqueue options, calling `withKey` for the static payload unless the overrides of the schedule give a key.
  For a task with it, it checks the enqueue overrides without calling the function, for errors that no option of the task can correct; each enqueue checks the merged options.
  The evaluator takes only the compiled schedule, so the public type does not carry the representation of the calendar.
- The application must not mutate a schedule or its nested options after `startWorker`: the worker compiles the calendar and copies the fields of the enqueue overrides and an array of scopes at start, but reads the payload and the objects of those fields, such as the `Date` of `runAt`, at each enqueue.
- Each enqueue uses the schedule's static payload, resolves the task options for that payload, and applies the schedule's enqueue overrides.
  Only the task, by its options or `withKey`, or the schedule gives the job a key.
- The handler may run on another worker in the same schema.

## Scopes

A schedule of a scoped task enqueues the task in each of its scopes, so that the job of each scope has its own attempts, backoff, and failure, and its handler, or `runHandler` of the worker, reads its scope as `job.scope` ([decision](../decisions/schedules/scopes-of-a-schedule.md)).

- `scopes` of the schedule is an array of scopes, or a function without arguments that returns one or a promise of one, such as the tenants of a registry.
  A schedule of a scoped task must give it, and a schedule of another task must not: `toWorkerSchedule` throws `TypeError` for either, and for a `scoped` of the task that is not a boolean, since the type of a task may not tell whether it is scoped.
  The types require `scopes` for a task whose scope type is `string`, which `defineTask` gives with `scoped: true`, unless the call gives the payload type explicitly, which leaves the scope type at its default.
  `ScheduleOptions` takes the scope type as its third type argument, and without it describes the options of a task that is not scoped, which have no `scopes`, so that options typed by its two-argument form fit a schedule of such a task of either kind.
- A schedule takes no scoped batch task: `toWorkerSchedule` throws `TypeError` for one, since `enqueue_items` takes the lock of its key in each scope until the transaction ends, and the lock table of the server bounds their count.
- `toScheduleScopes` returns the scopes of a check from an array of the schedule, at the start of the worker, or from what the function returned, at each call: it throws `TypeError` for a value that is not an array and the errors of `checkScope` of `limits.ts`, which the jobs API runs too, for an element, so that one invalid scope rejects the whole list, and returns each scope once, sorted by UTF-16 code units.
- A check that enqueues in a list with scopes resolves the parameters of the enqueue once, and enqueues them in every scope of the list by one statement ([Enqueue](enqueue.md#a-job-in-each-of-many-scopes)), so its enqueues differ only by the scope.
  The guarantee is one enqueue in each scope, not one new row: with a key, the job of a scope merges into the pending job of the key of that scope by its key mode.
- A list without scopes enqueues nothing and runs no function of the options of the task, and the check still advances the cursor.
- The enqueues of every scope commit with the advance of the cursor or not at all: an error in one scope, such as a lock timeout behind a producer that holds the pending job of the key of that scope, rolls back the jobs of the other scopes too, and a later check tries the occurrence again while it is due.
  Once committed, the job of each scope is settled on its own.
- An occurrence enqueues in the list that the function returned to the check that enqueues it: a scope missing from it gets its job at a later occurrence, and the job of a scope removed after it still runs, unless `runHandler` completes it without running the handler.
- The function must be safe to call again and at once from several workers, and must bound its waits, such as for its client, by a timeout: the worker neither cancels it nor bounds its duration.
- Workers that list the same ID must give the same scopes or equivalent functions, as the rest of the definition ([Configuration changes](#configuration-changes)).

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
A check of a schedule with an array of scopes or without scopes runs in its own transaction, using the database time at the start of that transaction for both evaluation and enqueue; a check with a function of the scopes takes a second transaction when an enqueue is due, as described below.
Let M be the UTC minute containing that instant.

1. The worker inserts the schedule row if absent, setting `evaluated_through` to one minute before M so that only M is eligible on first registration.
2. It locks the row with `FOR UPDATE SKIP LOCKED`.
   If another transaction holds the lock, it skips this schedule.
3. It returns if M is at or before `evaluated_through`, the last minute checked, inclusive.
4. For an enabled schedule, it enqueues once if M matches or if a matching minute after the cursor is at most `catchUp.within` seconds old.
   Without `catchUp`, only M is eligible.
   One enqueue covers all eligible minutes so that a reconciliation runs once after downtime ([decision](../decisions/schedules/one-job-per-recovery-window.md)).
5. It sets `evaluated_through` to M, including when the schedule is disabled or no minute matched.

For scopes that a function returns, a check takes two such transactions around the call of the function, so that the check holds neither a connection, which the function may wait for when it reads through the same pool, nor the lock of the row while the function runs ([decision](../decisions/schedules/scopes-outside-the-transaction.md)):

1. The first transaction runs steps 1 to 4 without scopes, at its own time T.
   If no enqueue is due, it ends as any check, by step 3 or 5, and the function is not called, also for a disabled schedule.
   If one is due, it commits without an enqueue and leaves the cursor in place, but keeps the registration of a schedule that had no row.
2. The worker calls the function and checks its list; an error ends the check without an enqueue.
3. A second transaction runs steps 1 to 5 as of T rather than of its own start, with the cursor that it locks: it enqueues the list with the time T, advances the cursor to the M of T, and leaves the minutes after it to the next check.
   In step 2, it waits for the lock of the row, as long as its lock timeout lets it, rather than skips the schedule.

- Deciding as of T keeps an occurrence that was due in the first transaction due however long the function took: by its own start, a second transaction after the end of M would lose the occurrence of a schedule without catch-up.
- Both transactions decide under the lock of the row, by the cursor that they lock.
  Another worker can advance the cursor while the function runs: the second transaction then finds M covered and drops its list, or decides again for the minutes that the cursor leaves, with its list.
  It waits for a worker that holds the row, since that worker may check as of an earlier time and leave the occurrence due, which a later check may find past its catch-up window; a wait past the lock timeout fails the check with its error.
  A worker that checks a later minute first covers M by its own rule, so without a catch-up window that includes M the occurrence is skipped.
- Several workers can call the function for one occurrence; only the transaction that advances the cursor enqueues, with its own list.
- A check whose function fails leaves the cursor as it was, also of a schedule that it registered, so a later check can retry the occurrence while the cursor has not covered it and M lasts or its catch-up window includes it.
- One list serves the single enqueue of a catch-up window, however many occurrences it recovers.

- The transaction bounds each of its lock waits to one second, including registration, the lock of the row in a second transaction, and enqueue.
  An error before `COMMIT`, such as a lock timeout or a throwing `withOptions`, rolls it back.
  The worker reports it, or an error of the function of the scopes, as a schedule error and continues to its next schedule.
- A worker checks its schedules one at a time in every round.
  `stop` starts no further check and waits for the one in flight, also for the function of its scopes.
  Once `stop` gives up, the check in flight neither calls the function nor writes in a transaction that has not begun to write, takes no connection when its function returns after that, and a transaction that has begun to write may still commit in the background.
- The cursor advance commits atomically with the enqueue, including a merge into an existing pending job.
  After rollback or an unknown commit outcome, the next check reads the durable cursor.
  Completing or deleting the job leaves the cursor in place ([decision](../decisions/schedules/occurrence-cursor.md)).
- A job's `delay` counts from the evaluation time, including on recovery and after the function of the scopes, and `runAt` keeps its absolute meaning.

At 03:00:40, the 03:00 occurrence is the current minute M, so it is eligible even with a 30-second window if it has yet to be checked.
At 03:01:00, recovering that occurrence for an already registered schedule requires a window of at least 60 seconds.

## Configuration changes

- The row holds only the schedule ID and its inclusive cursor.
  The active calendar and catch-up window apply to unevaluated minutes, including a gap before a configuration change.
- Minutes checked while disabled remain processed after re-enabling.
- A schedule omitted from every manifest counts as down until a worker lists it again.
- Workers that list the same ID must use the same definition, including `enabled`, the payload, the enqueue options, and the scopes ([decision](../decisions/schedules/one-definition-per-id.md)).
  If definitions differ, such as during a rolling deployment, the worker that first commits the cursor advance decides which minutes it covers and whether to enqueue, so an occurrence of only one definition can be lost.
  A changed definition under a new ID registers its own row, so neither definition advances the cursor of the other, and both enqueue until the workers of the old one stop; it starts at the minute of its first check, like any new ID.
  To change a definition under the same ID, the application must stop every worker that lists the ID before a worker with the new definition starts.

## Accepted limitations

- A lock timeout can delay the next successful check past the matching minute.
  Recovery then requires a catch-up window that still includes the occurrence.
- A rolled-back first registration leaves no row, so the next check registers the schedule again at its own minute, and catch-up recovers no occurrence before it.
- Rows for schedules removed from every manifest remain in the database.
- A slow check delays the later schedules of its round, also by the function of its scopes, and a function that never settles stops the checks of its worker and keeps a `stop` without a timeout from resolving.
- Each worker sends at least one transaction per schedule every round, also for a minute that it has already seen checked.
- One scope whose enqueue fails or waits past the lock timeout delays the jobs of every scope of the schedule to a later check, which without catch-up must start within the minute of the occurrence.
- A check that waits with its scopes for the lock of the row fails with the error of the lock timeout when another worker holds the row longer, such as by an enqueue that waits for a producer or one of many scopes, also when that worker covers the occurrence and leaves the check nothing to enqueue.
- The transaction of a check grows with its scopes: in seven runs on a workstation, a check of 50,000 scopes took about 0.3 seconds for a task without a key and 0.7 to 1 second for merges into pending jobs of a key.

## Open questions

- There is no API for removing the row of a retired schedule.
- The worker gives the function of the scopes neither a timeout nor an abort signal.
