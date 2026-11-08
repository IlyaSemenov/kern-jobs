# Queues

## One running job per queue

A queue runs at most one job at a time per scope, also when a worker claims several jobs at once, since a unique index of active jobs rejects a second one.

- Graphile Worker, same: it runs jobs of one named queue serially ([`queueName`](https://worker.graphile.org/docs/library/add-job)).
