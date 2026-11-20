# Administration

Check whether a key has an active job, and act on many jobs at once; [retry by id, and delete of pending or failed jobs by id or by a filter](../architecture/administration.md) exist.

- Retrying the failed jobs of a task in one statement, rather than by id one by one.
- Running a pending job that waits for its backoff now, such as after a fix of its cause.

graphile-worker-orchid has `isJobActive`, which tells whether the job of a key is running or may still run, including a job that waits to run or for a retry ([docs](https://github.com/IlyaSemenov/graphile-worker-orchid/blob/v2.5.1/README.md#isjobactive)).
