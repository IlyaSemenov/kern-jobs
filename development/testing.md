# Testing

How to place and write the tests of kern-jobs.

## Placement

- Keep focused module tests beside their source: `foo.test.ts` and `foo.type-test.ts` test `foo.ts`.
- Test each behavior at the lowest level that can check it: a helper directly, a module that combines helpers by its own unit tests.
- Keep a test of how several modules work together beside the module that combines them, or beside the modules it covers until such a module exists, such as the randomized and process-level tests of the worker in `src/worker/`, which combines the SQL core and the drivers.
- Keep a test helper that builds or reads internals beside them in `src/<module>/testing.ts`, such as `src/core/testing.ts`, so that nothing outside `src/` imports from `src/`.
- Keep integration and package-boundary tests in `tests/`, importing the package by name: they see only the public API.
  Keep the harness that they share in `testing/`, importing the package by name too.
- Name runtime tests `*.test.ts`, compile-only type tests `*.type-test.ts`, and tests of the packed package `*.package-test.ts`, which `bun test` skips and the `test:package` script runs.
- Keep a benchmark beside the module it measures as `<topic>.bench.ts`, and run it with `bun --env-file=.env.test.local src/<module>/<topic>.bench.ts`; do not run benchmarks in CI.

## Structure

- Add a `describe` block where the file gives a reason for it: several APIs or behaviors in one file, or a fixture that belongs to some cases but not all.
  Name such a block after what it covers and keep its fixtures inside it.
- Distinguish several same-kind values by role rather than by order.
  When values differ only by order, number them with digits instead of ordinal words.
- Keep tests deterministic so a failure repeats on every run.
  Generate random inputs from an explicit seed and print the seed in failure messages so the failing input can be replayed.

## Database

- Create a schema per test with `createTestSchema` of `src/core/testing.ts`, which creates it by the migrations and drops it afterwards; for a test that creates another schema, such as by `migrateJobs`, name it with `unmigratedTable`, which `dispose` drops too.
- Test connections do not add the test schema to `search_path`: qualify the names in the SQL of a test, such as with `schema.table.name`.
- The tests of the core and the worker run on the driver of `kern-db/postgres-js`, and a test of the worker runs it on the driver of `kern-db/node-postgres`.
- Pass virtual time, `T(seconds)`, as `now` to the statements of the SQL core instead of waiting for real time.

## Concurrency

Order concurrent transactions explicitly rather than by timing:

- hold a transaction open on one connection and wait with `waitForLock` of `kern-db/testing` until another backend blocks on its lock;
- stop a statement after its snapshot at a breakpoint that the SQL core marks with `hook(name)`: open the connection with that hook and hold the breakpoint with `holdHook`;
- stop a transaction between its statements through the `pause` callbacks of its operation.

## Worker

- Run the runtime of a worker in a test with `startTestWorker` of `src/worker/testing.ts`, and wait for its events with `recordEvents` rather than for a time; gate handlers with promises that the test resolves.
- The seed of the randomized test `src/worker/fuzz.test.ts`, `FUZZ_SEED`, does not fix the order of async events: reproduce a defect that it finds with a deterministic test.
- The process-level scenarios in `src/worker/faults/` run worker processes of `src/worker/faults/worker.ts`; the restart scenario starts a temporary cluster from the binaries in `PG_BIN`, or in `pg_config --bindir` when it is not set.

## Package

`tests/package/install.package-test.ts` installs the dependencies of the packed package with npm from the registry and runs the quickstart of [docs/getting-started.md](../docs/getting-started.md), `tests/package/quickstart/quickstart.ts`, in a temporary database: it needs the network and a user of `DATABASE_URL` that can create databases.
