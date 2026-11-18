# Schedule extensions

- Add an optional IANA time zone when a task needs local civil time; define skipped and repeated local minutes.
- Add RRULE as another calendar alternative with its own canonical form and the same UTC occurrence identity.
- Add bounded multi-occurrence catch-up as an optional catchUp mode with an age and batch limit; visit selected minutes in order and commit the cursor only through the portion processed.
- Skip the database in the later rounds of a minute that a worker has seen checked, and check schedules concurrently, so that many schedules on many workers cost fewer transactions and a slow check does not delay the others.
- Report the lag of each schedule, such as the age of its cursor, so that monitoring tells which schedules fall behind.
- A payload or options computed from the occurrence must define which time a job that covers several missed occurrences represents: the latest matching minute or the time of the check.
- Compare the zone and RRULE semantics of [pg-boss](https://github.com/timgit/pg-boss/blob/12.38.1/docs/api/scheduling.md#rrule-expressions) and the bounded backfill of [Graphile Worker](https://github.com/graphile/worker/blob/v0.18.0/website/docs/cron.md#limiting-backfill) when implementing these modes.
