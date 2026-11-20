# A worker returns the jobs of its id under a lock of the id

Each worker holds a session-level advisory lock of its id in the session of its heartbeat.
A worker that takes the lock knows that the heartbeat session of the previous worker of its id has ended, such as with its process, so at its start it returns the active jobs of the id at once, instead of leaving them to the reaper until their leases expire:

```sql
select pg_try_advisory_lock(hashtextextended('kern-jobs:worker:["kern_jobs","web-1"]', 0));
-- then, before the first claim, the statement of the reaper with
where j.state = 'active' and j.locked_by = 'web-1'
```

A worker that cannot take the lock takes no jobs and reports it, until the worker that holds it stops.

## Alternatives

### Return by the id alone

A worker could return the active jobs of its id at its start without a lock: it would keep no state in its session, run behind a pooler in transaction mode, and send one statement less.
But the id would be a promise of the application that no other worker of the id runs, which nothing checks.
Two replicas started with one id, or a new instance of a dev server that starts before the old one stops, would return the jobs of a running worker: its handlers keep running while the jobs run again, and a returned job of a queue frees the queue for its next job.
With the lock, such a worker takes no jobs and reports the id.

### Liveness of every worker by its lock

The reaper could return the jobs of any worker whose lock it can take, whatever their leases, so the jobs of every dead worker, also of a random id, would return within a round of the reaper.
But a restart of PostgreSQL ends every session at once, and until each worker takes its lock again, a reaper that reconnects first finds every lock free and returns every running job, whose handlers keep running.
Requiring also a heartbeat older than a few intervals would bound that, but would leave a gain of a fraction of the lease.
GoodJob and Que hold a session-level advisory lock of each running job instead, so that any process takes the job of a dead one at once, whatever its identity; a restart of PostgreSQL frees such locks as well.

### A short lease

An application that restarts its worker often, such as in development, can pass a short `lease` instead, and the reaper returns the jobs within the lease and an interval of the reaper, with no lock.
But a task with a lease of its own keeps it, and a shorter lease aborts handlers at shorter pauses of the event loop.

## Cost

- The connection of the heartbeat must be a session of PostgreSQL of its own: behind a pooler in transaction mode, the lock stays on a server connection that other clients reuse, so another worker of the id that gets that connection takes the lock again and returns the jobs of a running worker, and one that gets another connection cannot start until the pooler closes that one.
- The return runs in the session of the heartbeat, beside its heartbeats, so that the lock outlives each of its statements.
- A returned job spends its attempt, as after its lease, since its handler may have done its work.
- A worker that loses its heartbeat connection holds no lock until it reconnects, and a worker that starts with its id in between returns its running jobs.
