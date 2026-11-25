# Hand-written worker statements are kern-orm templates

The hand-written statements that only the worker runs, such as the claims, the heartbeat, and the reaper, are templates of the worker `Db` ([Drivers](../../architecture/drivers.md#statements)).
They name the job table and the claim sequence through the worker ORM schema, give each parameter its type in its place in the statement, and decode their result columns by `select`.

## Alternatives

SQL text with numbered parameters cast from text, as `$n::text::int`, which the statements that also run on the client of the application keep, renders nothing when it runs and so looks cheaper.
But it names the table by string concatenation, keeps parameters apart from their places in the statement, and leaves the worker with two ways to write a statement.

## Cost

Templates cost nothing measurable.
Medians of 15 runs of `src/worker/worker.bench.ts` for each revision, SQL text at a25e32d with kern-orm 0.26 against templates with kern-orm 0.32, each in a Linux container with 2 CPUs against PostgreSQL 18 in a container with 2 CPUs on tmpfs without fsync:

- no-op handlers with 1 slot: 610 against 592 jobs/s;
- with 8 slots: 1683 against 1675 jobs/s;
- with 32 slots: 4868 against 4880 jobs/s;
- a heartbeat of 1000 claims: 13.9 against 14.3 ms.

The runs of the two revisions overlap in every measurement; the largest shift, 3% with 1 slot, about 50 µs a job, is within the spread of the runs.
