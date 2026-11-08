# Queue mutex by a unique index

A unique index on `(scope, queue)` of active rows makes a second active job of a queue impossible, whatever the claim statement saw in its snapshot.
A claim from a stale snapshot fails with `23505`, and the claim cycle retries it; the stress and randomized tests saw it at most once per run.
The index also frees the queue with any transition from `active`, including the reaper, so the mutex needs no fencing or cleanup of its own.

A filter in the claim statement alone, such as `NOT EXISTS` an active job of the queue, lets two workers claim two jobs of a free queue at once, and a batch claim can take several jobs of one queue.
Graphile Worker ran jobs of one named queue concurrently when its batch fetch, `localQueue`, was enabled, until 0.18.0 ([#621](https://github.com/graphile/worker/issues/621)).

A table of queue locks, a row per queue that a claim takes, also serializes claims, but needs its own fencing and its own recovery after a worker crash.
