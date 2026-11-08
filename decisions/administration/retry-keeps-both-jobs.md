# A retry keeps both jobs when a pending job of its key exists

When an application retries a failed job while a pending job of its key exists, the retry changes neither job and returns the id of the pending one, in every key mode.
A retry names one job that the operator chose to run again, while an enqueue of a key only asks for work on its entity, so the retry does not merge the job away: the operator deletes the failed job if the pending one does its work, or deletes the pending job and retries again.

Letting the pending job win, so that the retry deletes the failed one, as a fail before the last attempt yields to a pending job of mode `replace` or `debounce` ([Merge on failure](../settlement/merge-on-failure.md)), saves the operator that choice, since the pending job runs after the retry and covers the failed one as a signal of its key.
But a retry then deletes the job that it names without running it, and the pending job is not always the newer event: a job of the key can return to pending after a failure while the next job of the key runs and fails for good, so a retry of that next job would yield to an older event.

Merging by the key mode of the pending job, as fail does, would let the failed job win under `keep`, so that the first event still wins, but it moves the payload, the attempts, and the run time of one row to the other, or needs a transaction that deletes one row and returns the other, which the client of the application does not give.

Detaching the failed job from its key, so that both jobs run, deletes nothing, but runs an idempotent recomputation twice, the older payload possibly after the newer one.
Graphile Worker runs both: an enqueue of the key after the failure takes the key from the failed job, which `rescheduleJobs` then runs beside the new one ([source](https://github.com/graphile/worker/blob/v0.18.0/sql/000020.sql#L120-L133)).
