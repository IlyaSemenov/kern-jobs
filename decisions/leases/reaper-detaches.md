# The reaper detaches instead of merging

When the reaper returns an expired job with a key and a pending job of the key exists, it detaches the returned job from its key, and both jobs run.
Within one batch, of several rows of one key, the newest keeps the key and the others are detached.

Merging by the key mode, as [fail](../settlement/merge-on-failure.md) does, would avoid running an idempotent job twice after a worker crash.
It needs the competitor locked and each row decided on its own, while detaching keeps one statement per batch.
The reaper also does not know the outcome of the crashed handler, which a superseded job would report.

The cost is a duplicate execution after a crash: the returned job runs with its old payload, and the pending job runs with the new one.
