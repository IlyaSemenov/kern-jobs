# Updating kern-jobs

Run [`migrateJobs`](../getting-started#create-the-schema) at every deploy, before the new version of your application starts, such as in the script that applies its migrations.
It updates the schema that an earlier version of kern-jobs created, and does nothing in a schema that is up to date.

- Processes that run it at once, such as the instances of one deploy, apply each change once.
- The producers and the workers of the previous version of kern-jobs keep working on the updated schema, so the old release can run while the new one starts.
- A process of the previous version that runs `migrateJobs` on a schema that a newer version has updated changes nothing.
- A worker of the new version takes no jobs until the schema has its changes, and [reports](../workers/monitoring#report-errors) it until then.
