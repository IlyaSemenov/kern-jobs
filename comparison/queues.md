# Queues

## One running job per queue

A queue runs at most one job at a time per scope, also when a worker claims several jobs at once, since a unique index of active jobs rejects a second one.

- Graphile Worker, fails: with its batch fetch, `localQueue`, it ran jobs of one named queue concurrently before 0.18.0 ([graphile/worker#621](https://github.com/graphile/worker/issues/621)).
