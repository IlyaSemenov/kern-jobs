# One definition per schedule ID

Require every worker that lists a schedule ID to use the same definition.
Change a definition either under a new ID, so that the two definitions keep separate cursors, or under the same ID after every worker of the old definition stops.
The row of a schedule stores only its cursor, so a worker cannot tell another definition of the same ID from its own.

During a rolling deployment from `{ minute: 0 }` to `{ minute: 30 }` under one ID, an old worker can check 12:30 first, find no occurrence, and advance the cursor, so no worker enqueues the 12:30 job.
Under a new ID, neither definition advances the cursor of the other, and both can enqueue until the old workers stop; the new ID starts at the minute of its first check.
Stopping the old workers first avoids both the race and the overlap, but no worker checks the schedule until a new one starts.

## A fingerprint of the definition

The row could store a hash of the calendar, the catch-up window, `enabled`, the task, the payload, and the static options, so that a worker reports a definition that differs from the one that last advanced the cursor.
It detects the conflict but cannot tell the old definition from the new one, so it reports every rolling deployment until the old workers stop, and it cannot hash the function of `withOptions`.

## An ordered revision

The schedule could take a revision that the application increments with each change: a worker of a higher revision takes over the row, and a worker of a lower revision stops checking it.
Occurrences can still be lost in the minutes before the first new worker checks the schedule, and an application that forgets to increment the revision gets the race of a shared definition.
