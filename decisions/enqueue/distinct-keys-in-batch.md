# A key at most once per `enqueueMany`

`jobs.enqueueMany` rejects with `RangeError`, before it calls the client, two payloads whose options give the same stored key.
One `INSERT … ON CONFLICT DO UPDATE` cannot update a row twice, and PostgreSQL rejects such a statement with `21000`, so a batch with a repeated key needs a rule of its own, and each rule below departs from the enqueues of the payloads one after another, which the call of an application means.

## The last item of a key wins

The statement keeps every item without a key, and of the items of each key, the one of the greatest position, such as by `DISTINCT ON (key)` ordered by the key and the position descending.
A repeated key is allowed, but the result differs from the enqueues one after another whenever an earlier item counts:

- `keep` keeps the first payload of a new key, while the last item wins with its own;
- `[replace A, keep B]` leaves payload A with mode `keep`, which no single item gives;
- `resetAttempts` of an earlier item is lost.

## The first item of a key wins

Choosing the first item of each key before the statement, and merging it into the pending job of its key by the usual upsert, keeps the first item.
pg-boss keeps the first job of a `singletonKey` in `insert` into a queue of the policy `short` ([source](https://github.com/timgit/pg-boss/blob/12.38.1/test/insertTest.ts#L238-L263)), and BullMQ the first of the jobs of a `jobId` without a deduplication id in `addBulk` ([source](https://github.com/taskforcesh/bullmq/blob/v6.7.5/src/commands/includes/handleDuplicatedJob.lua#L27-L30)).
A repeated key is allowed, but the later items of a key are lost, which the enqueues one after another would merge in `replace` and `debounce` modes.

## Folding the items of a key

Folding the items of a key into one event in JavaScript keeps the result of the enqueues one after another for one mode without debounce, but not for debounce over an existing window: two events demand `greatest(least(now + d1, w + m1), least(now + d2, w + m2))` for the start `w` of the window, which no single `delay` and `maxWait` give for every `w`, and the window of the row is known only in the statement.

## A statement per repetition

The `n`-th item of each key in the `n`-th statement gives the result of the enqueues one after another, but a call takes a statement per repetition, and its jobs commit together only in a transaction of the application.
