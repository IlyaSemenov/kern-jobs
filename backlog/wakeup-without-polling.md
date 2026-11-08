# Wakeup without polling

Waking workers on new jobs with LISTEN/NOTIFY and at the nearest `run_at` with a timer, with polling as a fallback.

Preliminary solution:

- One LISTEN per worker process, behind a small interface with an implementation for each client.
- One timer at the earliest `run_at` of the pending jobs of the tasks of the worker, moved earlier by a notification of an earlier job and recomputed after a claim that returned fewer jobs than it asked for.
- Polling as a fallback, and a claim at start.
- A job starts no earlier than its `run_at` and no later than its `run_at` plus the poll interval.

Open: reconnecting the LISTEN after a restart of the server, also with node-postgres on Bun.

Tests: a job delayed by 3 seconds starts within 3 to 4 seconds with a poll interval of 30; an earlier job moves the timer; without notifications, a job starts within the poll interval; an enqueue wakes a worker once its transaction commits.

Graphile Worker wakes its workers on a new job by LISTEN/NOTIFY, and finds jobs that become due later by polling, every 2 seconds by default ([docs](https://github.com/graphile/worker/blob/v0.18.0/website/docs/faq.md?plain=1#L22-L33)).
pg-boss wakes the workers of a queue by LISTEN/NOTIFY when the instance has `useListenNotify` and the queue has `notify: true` ([docs](https://github.com/timgit/pg-boss/blob/12.38.1/docs/api/workers.md#low-latency-dispatch-with-listen-notify)).
It notifies only of a job that is ready at once, and finds delayed jobs by polling ([docs](https://github.com/timgit/pg-boss/blob/12.38.1/docs/api/workers.md?plain=1#L313)).
