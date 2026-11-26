# `enqueueItems` enqueues the items of a batch task

A batch task takes its own method, `jobs.enqueueItems(task, items, overrides)`, which resolves to `{ id, created }` of each job that took items, and `jobs.enqueue` and `jobs.enqueueMany` reject a batch task, so they keep one result of each job.
`withBatchPayload` returns a `BatchTask`, whose `maxItems: number` keeps it from the `Task` that `enqueue` takes, and the SQL function `enqueue_items` appends items beside `enqueue`, which returns one row.

## `enqueue` of either kind, resolving to an array

`jobs.enqueue` could take a task of either kind and resolve to an array of `{ id, created }`, of one job for a task that is not a batch task, and `jobs.enqueueMany` to such an array for each payload, which also appends to several keys in one statement.
One method serves both kinds and code generic over tasks, but every call of a task that is not a batch task reads its one result from an array, which `noUncheckedIndexedAccess` types as possibly absent, and the SQL function `enqueue` would return rows of either count.

## A result type by the kind of the task

`jobs.enqueue` could resolve to `EnqueueResult` for a `Task` and to `EnqueueResult[]` for a `BatchTask` by a conditional type.
A call of a known task gets the type of its kind, but code generic over tasks gets the conditional type, from which neither the id nor an item is read without a cast, and one name would take a payload or items by a definition elsewhere.

## Appends to several keys in one call

An `enqueueItemsMany` of groups of items by key would append to several keys in one statement, resolving to an array of results for each group.
It takes the locks of several keys in one statement, which needs one order of the keys to avoid deadlocks, and no use is known; several calls of `enqueueItems` in one transaction are all or none, by a round trip each.
