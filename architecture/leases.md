# Leases

A claim gives the job a lease, which heartbeats extend until the job is settled.
The reaper returns jobs whose lease expired.

## Heartbeat

A heartbeat sets `lease_until = greatest(lease_until, now + lease)` for the active rows of the given claims, each with the lease of its own task, and never shortens a lease.
It returns by claim id:

- confirmed: the claims whose leases it extended;
- busy: the claims valid in its snapshot whose rows another transaction held or changed, which it neither extended nor lost;
- any other claim is lost, and the worker aborts its handler.

A heartbeat skips a row that another transaction holds instead of waiting for it, since a fail transaction holds the row of its claim while it waits for the pending job of its key, which a producer can hold for as long as its transaction lasts, and one heartbeat carries every claim of the worker ([decision](../decisions/leases/heartbeat-skips-locked-rows.md)).

The claim id decides ownership: a heartbeat or complete after the lease expired but before the reaper returned the job succeeds, and the heartbeat makes the reaper skip the row ([decision](../decisions/leases/ownership-by-claim.md)).

A worker keeps its leases with these requirements, which [the worker](worker.md#leases) implements:

- It sends heartbeats on a connection of their own, so that slow claims and settlements do not delay them ([decision](../decisions/leases/heartbeat-connection.md)).
- It sends one heartbeat at a time.
- It keeps heartbeating a claim until its settlement ends, or until the timeout of its `stop` passes ([Worker](worker.md#stopping)).
- It aborts a handler whose lease it could not confirm, a margin before the lease ends.
- It does not claim while its heartbeat connection is broken or a heartbeat is stuck, since it could not keep a new lease and would burn an attempt of every job it claims.

## Reaper

The reaper returns expired jobs in batches, by one statement per batch:

- It selects active rows whose lease expired, by id, with `FOR UPDATE SKIP LOCKED`, so a row that a heartbeat holds is skipped.
- A row on its last attempt becomes `failed` with `failed_at` now and keeps its key.
- Any other row returns to pending with `run_at`, `not_before`, and `window_started_at` now.
- Every returned row gets `last_error` `lease expired`.
- The reaper never merges: a row that returns to pending loses its key when a pending row of its key exists, or when the batch returns several rows of its `(scope, task, key)` to pending, of which the newest keeps the key ([decision](../decisions/leases/reaper-detaches.md)).

A producer can insert a pending row of a key after the snapshot of the statement; the statement then fails with `23505`, and the reaper redoes the batch row by row.
A row that still conflicts after three attempts loses its key unconditionally, so a hot key cannot stop the recovery of other rows.

A producer can also hold an uncommitted pending row of the key of a returning row, which makes the statement wait for the end of that transaction while it holds every row of its batch, so that no other reaper can return them.
The statement bounds each such wait by a `lock_timeout` of one second, which it sets itself, local to its transaction, so that it holds on any connection of the pool and leaves no setting on it.
After the timeout the reaper redoes the batch row by row, each row in a statement of its own with the same bound:
a row that times out stays active and expired, and the next round returns it once the producer ends; the other rows return in the same round ([decision](../decisions/leases/reaper-bounds-its-wait.md)).
A producer that holds a key for longer than the timeout costs the round the timeout for the batch and once more for the row of the key.

Returning a queued job frees its queue.
A reaper statement applies to its whole batch or to none of it, also when the reaper process dies during the statement.

## Fencing

Each claim gets a new claim id from a sequence, and every transition from `active` matches it ([decision](../decisions/leases/claim-id-fencing.md)).
After the reaper returns a job, the old claim can neither heartbeat, complete, fail, nor requeue it, also when the same worker claims the job again.
A complete that meets a row locked by the reaper waits for its transaction and finds the claim lost if that transaction commits; a heartbeat reports the claim busy while the reaper holds the row, and a heartbeat after the commit finds the claim lost.

## Accepted limitations

- After a worker crash, a job with a key can run twice: the returned job without its key and the next pending job.
- A handler that ignores the abort keeps running after its lease is lost and can overlap the next execution of its job, including in a queue; only the abort and the fencing of its settlement protect the job.
- When a worker dies during a claim statement, its server backend can still commit the claim; the job then spends an attempt and returns after its lease.
- A row whose key a producer transaction holds for long stays active, and every round waits the timeout on it again; many such rows in the oldest `limit` expired rows delay the return of the later ones.

## Open questions

- The reaper could merge by the key mode, which the row carries, instead of detaching.
