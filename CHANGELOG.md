# kern-jobs

## 0.2.0

### Minor Changes

- dc58686: Add `defineSchedule` to enqueue a task on a UTC cron calendar from a worker manifest, with optional catch-up after downtime.

### Patch Changes

- f1d6d29: Validate task names, scopes, keys, queues, worker IDs, and schema names for characters that PostgreSQL cannot store as given.

## 0.1.0

### Minor Changes

- d129a69: Initial release.
