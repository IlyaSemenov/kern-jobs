# Workers across tenant schemas

A user can restore a company's database snapshot in place, including the jobs and schedule cursors that belonged to its data at that point in time.
Other companies in the same database keep running.
A tenant schema can also be dumped with data, restored in a separate database, renamed to `public`, and served by the ordinary single-schema worker without rewriting queue rows.

The single-schema worker already takes a schema name.
Plan for 1,000 registered tenants with long idle periods and bursts of concurrent enqueues in one tenant.

## Storage and registration

- Each tenant owns its job table, identity and claim sequences, indexes, schedule cursors, and local migration history.
  Use independently restorable tables and indexes with names safe alongside application objects, preserving existing single-schema installations and their data.
  Keep tenant objects free of references to the coordinator and the root free of foreign keys into tenant job tables.
- Unscoped jobs keep the empty scope.
  Logical scopes remain available within a table.
  Schema isolation requires no routing fields in task names, keys, queue names, schedule IDs, or payloads.
  Applications may include their own business identifiers.
- A shared root schema holds a registration per tenant schema, with an immutable generation, lifecycle state, next inspection time, expiring claim-pass reservation and ownership token, and a scheduling version.
  An inbox holds individually identified wakeups for a generation at a database timestamp.
  Index due wakeups, due registrations, and expired reservations for bounded reads.
- Registration accepts a migrated schema, preserves its jobs and cursors, and marks it immediately due.
  Reconciliation of a live registration is idempotent.
  Replacing queue contents or reusing a schema name creates a new generation.
  Ordinary migrations retain the generation.

## Enqueue from the application or SQL

Select local or coordinated operation at installation and bind the root once for coordinated producers.
Keep the existing single-schema defaults and support a separate fixed queue for work outside tenant lifecycles.
The coordinated producer resolves the tenant's active registration, mutates its job, and appends a wakeup in the same transaction on the caller's session.
Use one SQL statement for query-only autocommit executors and preserve participation in existing transactions on all supported clients.
A missing registration or unavailable configured root must fail the operation.
Document which entry points guarantee this contract and which direct writes bypass it.

Build on the existing keyed upsert and preserve `{ id, created }`.
Use the stored eligibility returned by the mutation, including the effects of `keep`, backoff, and debounce windows.
A successful administrative `jobs.retry()` must atomically append a due wakeup even if the shard has no deadline.
Keep its `{ retried: true }`, `{ retried: false, pendingId }`, and `null` outcomes.

Keep registration admission, upsert, effective deadline, and wakeup expressible in PostgreSQL after the application has resolved its task options.
The [SQL function `enqueue`](../architecture/enqueue.md#sql-function) of an installation enqueues into the table of its own schema, which a dump of the tenant includes; a function of coordinated operation outside the tenant dump can then use the same write logic, with the installed body selecting local enqueue or enqueue with a root wakeup.
For example, an application migration could execute this SQL, the proposed common form of a call, in either installation:

```sql
select kern_jobs.enqueue(
  task => 'products.rebuild',
  payload => jsonb_build_object('id', p.id),
  key => 'product:' || p.id,
  queue => 'catalog',
  key_mode => 'debounce', delay => 2, max_wait => 10
)
from product p where p.active;
```

The function of coordinated operation, unlike the installed one, which writes into the table of its own schema whatever `current_schema()` of the caller, defaults the target to the caller's `current_schema()` and accepts an explicit `schema_name`.
Resolve that default at the API boundary and qualify and quote identifiers internally.
Install that function outside the tenant dump.

## Admission and discovery

1. Each producer appends its own inbox row.
   Transactions enqueueing distinct keys in one tenant must commit independently, including while workers update scheduling state.
   Account for foreign keys, triggers, and implicit row locks as well as explicit locks.
   Drain must wait for admitted transactions, and a waiting producer must establish that the generation is still active before mutating it.
   Closing admission requires a conflicting lock because a non-key state update taking `FOR NO KEY UPDATE` is compatible with a producer's `FOR KEY SHARE`. [PostgreSQL row lock compatibility](https://www.postgresql.org/docs/current/explicit-locking.html#LOCKING-ROWS)
2. Workers take bounded batches of due inbox rows with `FOR UPDATE SKIP LOCKED`, group by generation, update shards in consistent order, and delete exactly the selected IDs in one transaction.
   ID allocation can precede another transaction that commits first.
   Merge the earliest applicable deadline and advance the scheduling version so a pass already inspecting the shard cannot postpone a fresh wakeup.
   Bound inbox processing so claims for already discovered work keep progressing.
3. Every local transition that reveals work must commit a wakeup or already have a durable deadline or reservation that guarantees a timely revisit after process death.
   Cover admin operations, including delete of a queue head, claim, complete, release, requeue, retry merges, reaping, and schedule cursor advancement with enqueue.
   Derive eligibility from the surviving rows because completing one job can expose its successor and a merge can delete the failed job and retain its competitor.
   Preserve autocommit for each claim statement.
   An empty claim after skipping a locked row must allow rediscovery if the locking transaction rolls back.
4. Correctness relies on durable state only.
   Document the recovery bound for each crash window, including a committed local claim followed by process death before root bookkeeping.
   Once initial reconciliation finishes, polling uses root indexes and issues zero local queries for tenants with no work, lease, schedule deadline, or other reason for inspection.

## Workers and ownership

Use one concurrency limit per worker process and a connection budget independent of tenant count.
Reuse the claim cycle, heartbeat, settlement, and reaper primitives.
A reservation covers a bounded inspection and claim pass.
A tenant can fill spare slots and run handlers on several workers while other ready tenants receive turns.
Preserve plain/queued balancing and the named-queue cursor across passes.

Only the current reservation owner may postpone the next visit, conditional on the scheduling version it inspected.
An expired reservation must be recoverable without an enqueue.
Calculate the next visit from actionable work, delayed jobs, active leases, eligible queue successors, and schedule deadlines.
Reap and evaluate schedules while handler slots are full.
Preserve UTC calendars, disabled schedules, catch-up and atomic cursor advancement.
Avoid busy loops on blocked or unsupported jobs.
Reconcile new schedules on deployment even for previously idle tenants.
Define the manifest compatibility rule for rolling workers so one worker cannot hide jobs another can run.

Route every claim, grouped heartbeat, settlement, reaper operation, administrative call, and relevant diagnostic to its original table and generation.
Expose the actual schema at handler entry in both modes and leave tenant context setup to the application.
Include table identity in the advisory-lock namespace for named queues.
Maintain the existing heartbeat health and abort guarantees on bounded shared connections.

The reservation token fences shard bookkeeping, and the job claim ID fences settlement within its original table.
A handler can settle after its shard reservation has expired.
Fence stale generations at the actual SQL operation or through an enforceable lifecycle barrier.
A preflight followed by an unguarded statement can address a replacement table with the same name, job ID, and claim ID.

## Deployment

- **Fresh installation.** Initialize root explicitly, migrate and register each tenant, and force its first inspection.
  Prepare the queue before any application migration that enqueues jobs.
  The application owns its domain-table migrations.
- **Adopting existing local queues.** Close all old writer paths for the affected queues and stop their workers, including cron and reaping.
  Wait for in-flight producer transactions, handlers, and settlements to finish or terminate the old processes and resolve outstanding database transactions so they can no longer commit a local-only transition.
  Recover abandoned leases through the first inspection.
  Register or reconcile every affected schema as immediately due before reopening coordinated writes.
  This handoff needs no temporary discovery bridge.
- **Upgrading a coordinated installation.** Migrate root once and run `migrateJobs(target, { schemaName })` across registered tenants with bounded concurrency before rolling out new producers or workers.
  The previous release must remain compatible with original and upgraded layouts throughout the pass, including root.
  A failed migration identifies the tenant and migration and blocks the new rollout.
  Rerun using the local migration histories.
  Serialize activation of new or restored tenants with the migration snapshot and cutover so an old provisioner cannot subsequently activate an old layout.
  Existing active tenants continue operating during this pass.
- A new worker checks the local migration history before its first use of each registration generation.
  Cache success per worker release and generation, report a behind or missing schema with its identity, and retry at a bounded rate while healthy tenants continue.
  Keep DDL in deployment and provisioning.

## In-place restore

The application owns backup, replacement, and the durable progress of the restore operation.
Keep its orchestration jobs outside the tenant snapshot.
The application must explicitly invoke a tenant lifecycle operation even when the schema object and tenant identity survive the replacement.

1. Durably suspend the target registration, close producer admission and new claim passes, and quiesce that tenant before destructive DDL.
   Account for producers, claims, heartbeat, settlement, and maintenance across all workers.
   The application must also wait for or stop its handlers and prevent application writes during replacement.
   Other tenants stay serviceable.
   Generation fencing cannot reverse an external effect already performed by a handler.
2. Replace the local objects under suspension and verify or migrate the restored queue to the installed layout.
   Record enough durable evidence outside the replaced objects to distinguish a confirmed rollback, a committed replacement awaiting activation, and an unknown or partial outcome.
   For a transactional replacement, its commit can include a receipt in the same database transaction.
3. After a verified replacement, activate a fresh generation and mark it immediately due as one durable operation.
   Retrying activation for the same restore must return the same generation, including after a lost commit acknowledgement.
   The first inspection discovers pending and delayed jobs, active leases, named-queue successors, and restored schedule cursors.
   Old wakeups, reservations, claims, and late SQL from the previous generation must be harmless.
4. A proven rollback or failure before replacement permits resuming the original generation after quiescence, with an immediate inspection.
   After a crash between replacement commit and activation, keep the tenant suspended until idempotent recovery completes.
   If destructive writes may have committed partially, require the application to establish a consistent queue before activation.
   A timeout, unchanged schema name, or expired reservation cannot establish the outcome of a restore.

Removal uses the same admission and quiescence guarantees before destructive DDL or reuse of a schema name.

## Verification

Use PostgreSQL integration tests with controlled transaction barriers.
Verify the two restore contracts separately:

- **Restore in place.** Snapshot tenant A with delayed job X, a schedule cursor, and an active lease.
  After the snapshot, run X, enqueue Y, and retain old-generation wakeups and claim references.
  Quiesce A and replace its tables from the snapshot under the same schema name while tenant B executes jobs.
  Verify that X is discovered and runs at its restored eligibility, Y is absent, the lease is recovered and the cursor continues.
  Exercise colliding job and claim IDs and late operations from before replacement.
  Verify B makes progress during suspension, then force rollback, a crash after replacement commit, and a lost activation acknowledgement.
  Repeat recovery and verify exactly one activation.
  Simulate an uncertain or partially committed replacement and verify the tenant stays suspended.
- **Portability.** Run a real data-containing `pg_dump --schema=<tenant>`, restore to a separate database, rename the restored schema to `public`, and use the ordinary worker without root.
  Verify migration history, deduplication against a restored delayed job, named queue serialization, handler schema context, and schedule cursor continuation without any queue-data rewrite.

Additional acceptance cases:

- Hold producer A open after enqueue, let B commit and run a distinct key in the same tenant, then roll A back.
  Overlap a scheduling update, an admitted producer, and drain.
  Prove drain waits, and a producer waiting behind it cannot enter the closed generation.
- Force local and root write failures for enqueue and administrative retry with transactional and query-only autocommit clients.
  Retry a failed job when the shard has no deadline and verify its immediate discovery.
  A failed wakeup insert must leave it failed.
  Preserve all three retry outcomes and check stored deadlines for keyed merges.
- Invert inbox ID allocation and commit order, race inbox consumers, apply a wakeup during inspection, and finish an expired pass after its successor.
  Interrupt worker transitions at their commit boundaries, including queue completion and keyed retry, and verify recovery without an unrelated enqueue.
- Exercise colliding tenant-local IDs, keys, queues, and schedules, expired leases, rollback of a locked ready row, and a stale claim after removal and name reuse.
  Check fair progress, hot-tenant capacity across workers, maintenance with occupied slots, and rollout of changed manifests.
  Instrument steady-state SQL against 1,000 idle registrations.
- Test a failed and resumed migration pass, the previous release on upgraded schemas, a behind tenant alongside healthy ones, and activation racing cutover.
  Hold a legacy writer in flight at adoption and verify that final discovery includes its committed work without historical wakeups.
  Confirm that schema upgrades preserve the generation.
  Include existing single-schema, package, migration, and worker fuzz coverage.

Benchmark the shared table with logical scopes, an uncoordinated local table, and a coordinated local table under the same hot-tenant workload with distinct keys.
Include long application transactions and concurrent shard reservations.
Record throughput, p50 and p99 through commit, lock waits, inbox production, cleanup and backlog under sustained load, along with database, hardware, pool, concurrency, key distribution, and duration.

## Open questions for implementation

- Which public names and collision-safe local object names preserve upgrades and keep existing application objects distinguishable?
- Which admission locks and lifecycle operations enforce the required ordering on every supported client?
  Establish lock compatibility and crash recovery for each mutation before implementing its SQL.
- How should workers with different task manifests represent eligibility and bound discovery latency without repeatedly probing tenants?
  Document deployment assumptions, recovery bounds, measured costs, and the application's restore responsibilities in the public and architecture guides when implemented.
