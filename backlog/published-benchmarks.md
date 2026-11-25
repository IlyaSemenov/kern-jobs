# Published benchmarks

Publish in the user guide what throughput and latency kern-jobs reaches on stated hardware, and what the queues of [comparison/](../comparison/README.md) reach on the same workload.

## Preliminary decisions

- Run PostgreSQL with durable settings, `fsync` and `synchronous_commit` on: every settlement waits for its commit to reach the disk, which bounds the throughput of few slots.
  In a container on a shared host, a worker with 1 slot completed 78 no-op jobs a second with them and 600 without them.
- State the CPU, memory, and disk, the versions of PostgreSQL and the runtime, the settings of PostgreSQL, and the sizes of the pools.
- Measure the workload of an application rather than the overhead that `src/worker/worker.bench.ts` measures: the throughput of enqueues, the delay from the commit of an enqueue to the start of its handler, and the throughput of completions by slots and by number of workers, with no-op handlers and with handlers that take a fixed time.
- Run the same workload on pg-boss and Graphile Worker at the versions that [comparison/](../comparison/README.md#projects) lists.

## Open questions

- Where the harness lives, since it installs and runs other queues: a directory outside the package, or a repository of its own.
- Whether to measure BullMQ, whose store is Redis.
- A page of its own on the site, with a summary in `docs/comparison.md`, or a section of `docs/comparison.md`.
