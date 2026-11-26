# Leases

A claim gives the job a lease, which heartbeats extend until the job is settled.
The reaper returns jobs whose lease expired, and a worker that starts with the id of an ended worker returns the jobs of that worker.

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
- It keeps heartbeating a claim until its settlement ends, or until its `stop` gives up at its timeout or with `force` ([Worker](worker.md#stopping)).
- It aborts a handler whose lease it could not confirm, a margin before the lease ends.
- It does not claim while its heartbeat connection is broken or a heartbeat is stuck, since it could not keep a new lease and would burn an attempt of every job it claims.

## Reaper

The reaper returns expired jobs in batches, by one statement per batch:

- It selects active rows whose lease expired, by id, with `FOR UPDATE SKIP LOCKED`, so a row that a heartbeat holds is skipped.
- A row on its last attempt becomes `failed` with `failed_at` now and keeps its key.
- Any other row returns to pending with `run_at`, `not_before`, and `window_started_at` now.
- Every returned row gets `last_error` `lease expired`.
- A row that becomes failed with the stamp of a failure job inserts it in the same statement, with the reason `lease-expired` ([Failure jobs](failure-jobs.md#insert)).
- The reaper never merges: a row that returns to pending loses its key when a pending row of its key that is not sealed exists, or when the batch returns several rows of its `(scope, task, key)` to pending, of which the newest keeps the key ([decision](../decisions/leases/reaper-detaches.md)).
- A batch job returns sealed with its key, outside the uniqueness of keys, so it is never detached, and the ranking of the rows of a key leaves it out.

A producer can insert a pending row of a key after the snapshot of the statement; the statement then fails with `23505`, and the reaper redoes the batch row by row.
A row that still conflicts after three attempts loses its key unconditionally, so a hot key cannot stop the recovery of other rows.

A producer can also hold an uncommitted pending row of the key of a returning row, which makes the statement wait for the end of that transaction while it holds every row of its batch, so that no other reaper can return them.
The statement bounds each such wait by a `lock_timeout` of one second, which it sets itself, local to its transaction, so that it holds on any connection of the pool and leaves no setting on it.
After the timeout the reaper redoes the batch row by row, each row in a statement of its own with the same bound:
a row that times out stays active and expired, and the next round returns it once the producer ends; the other rows return in the same round ([decision](../decisions/leases/reaper-bounds-its-wait.md)).
A producer that holds a key for longer than the timeout costs the round the timeout for the batch and once more for the row of the key.

Any other rejection of the batch, such as of the insert of a failure job, rejects the reap, which leaves its rows active for the next round.

Returning a queued job frees its queue.
A reaper statement applies to its whole batch or to none of it, also when the reaper process dies during the statement.

## Jobs of an ended worker

A worker holds a session-level advisory lock of its id in the session of its heartbeat for its life, so that a worker that starts with the id of an ended worker returns the jobs of that worker without waiting for their leases, such as after a restart of a process that was killed ([decision](../decisions/leases/worker-id-lock.md)).
PostgreSQL releases the lock when the session ends, so a session that takes it tells that the worker that held it has stopped, its process has died, or its connection was lost.

- Each running worker of an [installation](README.md#terms) must have an id of its own.
- The heartbeat connection must be a session of PostgreSQL of its own, not a connection that a pooler in transaction mode shares between clients: PostgreSQL keeps a session lock on the server connection that took it, so another client of that connection would take the lock again, and a client of another one would wait for a lock that no worker holds.
- The key of the lock names kern-jobs, the schema, and the id, so that workers of different installations may share an id.
- Each heartbeat takes the lock first, which kern-db answers without a statement in a session that holds it; a heartbeat that cannot take it fails, so the worker takes no jobs while another session holds the lock, such as of a running worker of the same id.
- Once a heartbeat holds the lock and the schema is checked, and before its first claim, the worker returns the active jobs of its id by the statements of the [reaper](#reaper), with `locked_by` of the id instead of an expired lease and `last_error` `worker restarted`: a job on its last attempt fails, inserting its failure job with the reason `worker-restarted`, and any other returns to pending, also before its lease expires.
- It runs these statements in the session of its heartbeat, which closes only after the statement in flight, so that a worker of the id that takes the lock next never meets a statement of the return of this one; once the stop begins, it sends no further statement of the return, also within a batch.

## Fencing

Each claim gets a new claim id from a sequence, and every transition from `active` matches it ([decision](../decisions/leases/claim-id-fencing.md)).
After the reaper returns a job, the old claim can neither heartbeat, complete, fail, nor requeue it, also when the same worker claims the job again.
A complete that meets a row locked by the reaper waits for its transaction and finds the claim lost if that transaction commits; a heartbeat reports the claim busy while the reaper holds the row, and a heartbeat after the commit finds the claim lost.

## Accepted limitations

- After a worker crash, a job with a key can run twice: the returned job without its key and the next pending job.
- A handler that ignores the abort keeps running after its lease is lost and can overlap the next execution of its job, including in a queue; only the abort and the fencing of its settlement protect the job.
- When a worker dies during a claim statement, its server backend can still commit the claim; the job then spends an attempt and returns after its lease.
- A row whose key a producer transaction holds for long stays active, and every round waits the timeout on it again; many such rows in the oldest `limit` expired rows delay the return of the later ones.
- An active row stays active while PostgreSQL rejects the insert of its failure job, such as for a payload that fits in `jsonb` on its own but exceeds the limit when wrapped in the `JobFailure`, or by a change of the table that the application must not make ([Administration](administration.md)), and fails every round whose batch includes it: the rows of the rejected statement stay active too, the whole batch or, in a per-row redo, the row alone, after which the round ends, and a worker that starts with the id of the worker of the row claims nothing.
- A job of an ended worker whose key a producer transaction holds past the timeout of every batch of the return stays active until the reaper returns it after its lease; when producers hold the keys of every row of a batch, the return ends, and the jobs after them wait for their leases too.
- A worker whose heartbeat session ended while its process runs, such as after the loss of its connection, holds no lock until a heartbeat in its next session takes it again: a worker that starts with its id in between returns its jobs, whose handlers still run until the first worker aborts them at the deadline of their leases, since its heartbeat fails at the lock before it extends them, and the first worker takes no jobs while the second one runs.
- After `stop` gives up, its handlers may still run without the lock, and a worker that starts with its id returns their jobs at once.

## Open questions

- The reaper could merge by the key mode, which the row carries, instead of detaching.
