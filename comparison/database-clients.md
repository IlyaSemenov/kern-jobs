# Database clients

## Enqueue in a transaction of the application

`createJobs` takes the client of the application, or a function that returns the client of its current transaction, so a job commits with the transaction that enqueued it.

- pg-boss, same: `send` takes `db`, an object with `executeSql(text, values)` that resolves to `{ rows }`, and pg-boss ships such adapters for ORMs ([docs](https://github.com/timgit/pg-boss/blob/12.38.1/docs/api/jobs.md?plain=1#L76-L86)).
- graphile-worker-orchid, same: its `addJob` runs on the connection of Orchid ORM and joins its current transaction ([docs](https://github.com/IlyaSemenov/graphile-worker-orchid/blob/v2.5.1/README.md#addjob)).
- Graphile Worker, differs: an application calls the SQL function `graphile_worker.add_job` on its connection ([docs](https://github.com/graphile/worker/blob/v0.18.0/website/docs/sql-add-job.md)).

## The client of the worker

The worker runs on postgres.js or node-postgres, through a driver of kern-db.

- Graphile Worker, lacks: it runs on `pg` ([source](https://github.com/graphile/worker/blob/v0.18.0/package.json#L59)).
- pg-boss, differs: it runs on its pool of `pg`, or on an object with `executeSql` that its constructor takes as `db` ([docs](https://github.com/timgit/pg-boss/blob/12.38.1/docs/api/constructor.md#db)).
