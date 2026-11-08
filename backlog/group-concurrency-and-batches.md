# Group concurrency and batches

More than one active job per scope or queue, fairness between scopes, and a handler that takes several pending jobs of one scope at once, up to a batch size.
No use case yet; the schema needs only an index on `(scope, run_at)` of pending rows, which a migration adds.

The `groupConcurrency` option of `work` in pg-boss is a limit of the active jobs of a group that `send` names.
A fetch gives back the jobs over the limit after its claim commits ([release notes](https://github.com/timgit/pg-boss/releases/tag/12.38.1)).
A job that the policy of its queue refuses to give back runs over the limit.
A fetch in a transaction of the application can exceed the limit.
