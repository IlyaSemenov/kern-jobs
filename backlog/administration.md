# Administration

Delete a pending job by its key, check whether a key has an active job, act on many jobs at once, and count jobs; [retry and delete by id](../architecture/administration.md) exist.

- Retrying the failed jobs of a task in one statement, rather than by id one by one, and deleting the failed jobs older than a given age through the API rather than by the statement of the user documentation.
- Running a pending job that waits for its backoff now, such as after a fix of its cause.
- Counts of jobs by state and the age of the oldest pending job, by task and scope, for metrics, equal to those of a direct query of the table.

Other projects delete failed jobs in bulk: Graphile Worker every job that failed for good by its `DELETE_PERMAFAILED_JOBS` cleanup ([source](https://github.com/graphile/worker/blob/v0.18.0/src/cleanup.ts#L35-L41)), pg-boss a failed job `deleteAfterSeconds` after it failed ([source](https://github.com/timgit/pg-boss/blob/12.38.1/src/plans.ts#L4015-L4023)), BullMQ the failed jobs older than a grace period by `clean` ([source](https://github.com/taskforcesh/bullmq/blob/v6.7.5/src/classes/queue.ts#L780-L843)), and Sidekiq the dead jobs beyond 10,000 or 180 days ([source](https://github.com/sidekiq/sidekiq/blob/v8.1.7/lib/sidekiq/config.rb#L35-L36)).

graphile-worker-orchid removes the pending job of a key with `removeJob` ([docs](https://github.com/IlyaSemenov/graphile-worker-orchid/blob/v2.5.1/README.md#removejob)).
Its `isJobActive` tells whether the job of a key is running or may still run, including a job that waits to run or for a retry ([docs](https://github.com/IlyaSemenov/graphile-worker-orchid/blob/v2.5.1/README.md#isjobactive)).
