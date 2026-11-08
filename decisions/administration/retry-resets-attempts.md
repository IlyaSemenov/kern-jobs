# A retry gives a full budget of attempts

A retry of a failed job sets its attempts to 0, so it runs up to its `max_attempts` again, with backoff between them: the operator retries a job once its cause is fixed, and the retry starts a new cycle of attempts, as of a new job.
A handler sees the attempt from 1 to `maxAttempts` again.

Keeping the attempts gives a job that spent them one more attempt, so a retry whose cause is still there, such as of many failed jobs retried before their fix is deployed, ends after one run instead of a series of attempts with backoff.
But one transient error then fails such a job for good again, and a job that a `TerminalError` failed before its last attempt keeps only the rest of its budget; raising `max_attempts` by one instead keeps the handler from seeing an attempt above it.
pg-boss keeps the count of retries and raises their limit by one ([source](https://github.com/timgit/pg-boss/blob/12.38.1/src/plans.ts#L4150-L4166)), Sidekiq retries a dead job with its retry count less one ([source](https://github.com/sidekiq/sidekiq/blob/v8.1.7/lib/sidekiq/api.rb#L642-L650)), and BullMQ keeps the attempts unless `job.retry` is told to reset them ([docs](https://github.com/taskforcesh/bullmq/blob/v6.7.5/docs/gitbook/guide/jobs/retrying-job.md#L119-L241)), so in each, a job that spent its attempts runs once more.

A retry has no backoff either: it runs as soon as a worker is free, since the operator retries it once the cause is fixed.
