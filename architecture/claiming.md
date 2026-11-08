# Claiming

A worker claims ready jobs of the tasks it implements in claim cycles.
Each claim statement runs in its own autocommit transaction.

A claim makes a pending job active with a new claim id, increments `attempts`, and sets `lease_until` to now plus the lease of its task, from a map of task leases that the worker passes.
It returns the claim lag, the claim time minus `run_at`, so that late claims are visible.

A claim does not check `attempts` against `max_attempts`, so a job whose budget a `replace` lowered below its spent attempts runs once more instead of staying pending forever.

## Jobs without a queue

The worker claims a batch of the earliest ready jobs with `FOR UPDATE SKIP LOCKED`, skipping rows that producers or other workers hold.

## Queues

A queue keeps the [invariants of queues](data-model.md#invariants): one active job per `(scope, queue)`, started in the order of `(run_at, id)` among the jobs ready in the snapshot of the claim.
A retry gets a new `run_at` and waits in the queue by it.
The unique index on active rows enforces the mutex ([decision](../decisions/claiming/queue-mutex-index.md)).

One statement claims the head of one free queue:

1. A loose index scan visits the distinct `(scope, queue)` of pending rows, starting after the cursor of the worker ([decision](../decisions/claiming/queue-scan.md)).
2. A queue passes when it has a ready job of a known task, has no active job, and is not skipped in this cycle.
   These filters run in a subquery fenced by `OFFSET 0`, and `pg_try_advisory_xact_lock` runs outside it under `LIMIT 1`, so the statement holds at most one advisory lock under any plan.
3. The head, by `(run_at, id)`, is selected without a lock, and then only the head is locked with `FOR UPDATE SKIP LOCKED`, rechecking `state`, `run_at`, and the queue, so a head that a producer moved into the future or into another queue after the snapshot does not start before its turn.
4. When another transaction holds the head, or the recheck rejects it, the statement returns `busy`, and the cycle skips the queue until its end instead of starting a later job of it ([decision](../decisions/claiming/busy-queue-head.md)).

A stale snapshot can still pick a job of a queue whose other job became active meanwhile; the unique index rejects the claim with `23505`, and the cycle retries, at most three times.

The advisory lock is held until the end of the transaction, so a claim statement inside an explicit transaction would keep other workers away from its queue until the commit: claim statements must run in autocommit.

## Claim cycle

A cycle fills the free slots of the worker:

- The slots are split in halves between jobs without a queue and queued jobs, and the larger half alternates between cycles.
- A class without ready jobs leaves its share to the other.
- Queues are visited round-robin from the cursor of the worker, which wraps around once per cycle.

Under a continuous inflow above capacity, neither class nor any queue starves ([decision](../decisions/claiming/slot-split.md)).

A cycle hands each claim to the worker as soon as its statement returns: the claim is committed, and if a later statement of the cycle failed, it would otherwise stay active until its lease expires.
Before each claim statement after the first, the cycle asks the worker whether to go on, so that a worker whose heartbeat failed meanwhile claims nothing more.

## Accepted limitations

- A reaped queued job gets `run_at` now and waits behind the jobs already ready in its queue.
- The head of a queue is chosen among the tasks that the claiming worker implements, so workers with different task sets on one queue do not guarantee the order between tasks.
- The order is that of the claim snapshot: a head that a producer moves after the snapshot to a later run time that is still due starts before a job that is now earlier, and the head starts before a job that a producer inserts or moves before it after the snapshot.
- A producer transaction that holds a due row delays only that row, and producers that hold it at every poll delay it indefinitely; the claim lag shows the delay.

## Open questions

- The queue scan visits the queues in order until it finds a free one; claiming with hundreds of busy queues is not measured.
- Whether many deferred pending jobs, such as 10,000 due in a day, slow the claim of ready ones is not measured.
- Without statistics of the table, such as before its first `ANALYZE`, the claim cycles of 200 steps of 20 queues with a growing backlog took about 10 times as long as after an `ANALYZE` on PostgreSQL 15; their plans are not examined.
