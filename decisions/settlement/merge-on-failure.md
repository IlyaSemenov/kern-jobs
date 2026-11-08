# Merge on failure by the key mode of the pending job

When a job with a key fails before its last attempt while a pending job of its key exists, fail merges the two by the key mode of the pending job:

- `replace` or `debounce`: the failed job is deleted as superseded, since the pending job carries a newer event and recomputes the same state.
- `keep`: the pending job is deleted, and the failed job returns to pending with its retry time, since the first event wins.

Each job keeps its own attempts, and a terminal outcome is always recorded as `failed`.

Always detaching the failed job from its key, so that it returns to pending without the key and both jobs run, loses nothing, but runs a job of an idempotent recomputation twice.

Moving the attempts of the failed job to the pending job, as the maximum of both, would make a poisoned job spend the budget of the next event, and a stream of events could never retry the job for good.

The pending job carries the mode of its latest enqueue, so the newest producer code decides the merge during a rolling deploy.
The worker passing its own mode would apply the code of whichever worker version fails the job.
