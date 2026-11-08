# Enqueue from SQL

A SQL function with the options and key modes of an enqueue, so that a migration of the application can enqueue jobs, such as `search.indexProduct` for each existing product, without knowing the layout of the job table.

- The function comes with a migration of the schema of kern-jobs, so an application calls `migrateJobs` before its own migrations.

Graphile Worker enqueues from SQL with the function `graphile_worker.add_job`, which its `addJob` calls too ([docs](https://github.com/graphile/worker/blob/v0.18.0/website/docs/sql-add-job.md)).
