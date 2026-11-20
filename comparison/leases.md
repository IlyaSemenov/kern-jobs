# Leases

## Jobs of a dead worker

A worker extends the leases of its running jobs, 60 seconds long by default.
When a worker dies, each of its jobs returns to pending once its lease expires, or fails if that was its last attempt; a worker that starts with the id of the dead worker returns them at once.

- Graphile Worker, differs: a job of a worker that died stays locked for at least 4 hours ([docs](https://github.com/graphile/worker/blob/v0.18.0/website/docs/error-handling.md#instantaneous-exit)), unless the application passes the ids of crashed workers to `force_unlock_workers` ([docs](https://github.com/graphile/worker/blob/v0.18.0/website/docs/admin-functions.md?plain=1#L91-L125)); a worker that starts with a configured `workerId` unlocks nothing.
- graphile-worker-orchid, differs: `unlockAllJobs` unlocks every job at once, which is safe only while no worker runs ([docs](https://github.com/IlyaSemenov/graphile-worker-orchid/blob/v2.5.1/README.md#unlockalljobs)).
- pg-boss, differs: a heartbeat is an option of a queue or a job, and without it a job of a worker that died waits for its expiration, 15 minutes by default ([docs](https://github.com/timgit/pg-boss/blob/12.38.1/docs/api/queues.md#heartbeat-vs-expiration)).
- BullMQ, differs: a job holds a lock of 30 seconds that its worker renews, and a stalled-job check every 30 seconds moves a job whose lock is gone back to wait, or fails it after `maxStalledCount` stalls ([docs](https://github.com/taskforcesh/bullmq/blob/v6.7.5/docs/gitbook/guide/workers/stalled-jobs.md?plain=1#L1-L13)).
- Sidekiq, lacks: its open-source fetch takes a job off Redis, so a job that runs when its process dies does not return to its queue ([source](https://github.com/sidekiq/sidekiq/blob/v8.1.7/lib/sidekiq/fetch.rb#L48-L72)); `super_fetch` of Sidekiq Pro returns the jobs of processes whose heartbeat expired when a process starts, with no bound on the delay ([wiki](https://github.com/sidekiq/sidekiq/wiki/Reliability/b685f8063a1147679a5221f675433d40588bdaff)).
- GoodJob, has: by default, a process holds a session-level advisory lock of each job it runs, so another process can take a job of a process that died at once, whatever its identity ([docs](https://github.com/bensheldon/good_job/blob/v4.24.1/README.md?plain=1#L1645-L1651)), and runs it again ([docs](https://github.com/bensheldon/good_job/blob/v4.24.1/README.md?plain=1#L1213-L1232)).
- Que, has: a worker holds a session-level advisory lock of each job it runs, which PostgreSQL releases when its process dies, so another worker can take the job at once ([docs](https://github.com/que-rb/que/blob/v2.4.1/docs/README.md?plain=1#L466-L472)).
- Solid Queue, differs: a process heartbeats every 60 seconds and counts as dead after 5 minutes, and pruning it fails its claimed jobs without a retry ([docs](https://github.com/rails/solid_queue/blob/v1.7.2/README.md?plain=1#L404-L414)).
- Oban, differs: `Lifeline` returns a job that has been executing for longer than `rescue_after`, 60 minutes by default, whether its node runs or not, or discards it when its attempts are exhausted ([source](https://github.com/oban-bg/oban/blob/v2.24.2/lib/oban/lifeline.ex#L1-L76)).

## Two running workers with one id

A worker whose id another running worker of its installation has takes no jobs, and reports it, until that worker stops.

- Graphile Worker, lacks: its documentation asks not to give workers the same `workerId`, and nothing checks it ([source](https://github.com/graphile/worker/blob/v0.18.0/src/interfaces.ts#L762-L766)).
- pg-boss, differs: `instanceName` need not be unique, and it names instances only for observation ([docs](https://github.com/timgit/pg-boss/blob/12.38.1/docs/api/constructor.md?plain=1#L313-L335)).
