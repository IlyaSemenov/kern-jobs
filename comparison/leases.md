# Leases

## Jobs of a dead worker

A worker extends the leases of its running jobs, 60 seconds long by default.
When a worker dies, each of its jobs returns to pending once its lease expires, or fails if that was its last attempt.

- Graphile Worker, differs: a job of a worker that died stays locked for at least 4 hours ([docs](https://github.com/graphile/worker/blob/v0.18.0/website/docs/error-handling.md#instantaneous-exit)).
- graphile-worker-orchid, differs: `unlockAllJobs` unlocks every job at once, which is safe only while no worker runs ([docs](https://github.com/IlyaSemenov/graphile-worker-orchid/blob/v2.5.1/README.md#unlockalljobs)).
- pg-boss, differs: a heartbeat is an option of a queue or a job, and without it a job of a worker that died waits for its expiration, 15 minutes by default ([docs](https://github.com/timgit/pg-boss/blob/12.38.1/docs/api/queues.md#heartbeat-vs-expiration)).
