# Placement of the job table

Other placements of the job table besides a PostgreSQL schema of its own:

- a table in a schema of the application, beside its own tables;
- a table in the schema of each tenant with the jobs of that tenant only, with one worker for all tenants, such as by sharding;
- partitions of one job table by scope, also in the schema of each tenant, so that a dump of that schema carries its jobs and dropping the schema drops them.

Partitioning by scope needs `scope` in every unique index: the unique indexes of keys and queues have it, but the primary key on `id` alone must become `(scope, id)`, and statements that find a job by `id` should also pass its scope.
Its risks:

- creating a partition takes a brief `ACCESS EXCLUSIVE` lock;
- planning slows with thousands of partitions;
- `ON CONFLICT` of an enqueue must infer the partial unique index of keys on a partitioned table;
- `id` and the claim sequence stay global across the partitions.

Done when the whole suite passes on a table of three partitions.

Preliminary decisions for the migrations of the schema:

- The history applies to a schema of any name, beside other tables, with a table of versions of its own, so a table in a schema of the application or of each tenant needs only names of the table, the sequence, and the indexes that the application chooses; the migrations then become functions of these names.
- `migrateJobs` with the schema of a tenant keeps the versions and the lock of that tenant.
  An application that is not on kern-db cannot call it in the transaction that creates the tenant, so its first enqueue for a tenant comes after that transaction commits.
- With partitions, the history migrates the parent table, and creating the partition of a scope is an operation outside the history, in a short transaction of its own, since `CREATE TABLE … PARTITION OF` keeps its `ACCESS EXCLUSIVE` lock of the parent until its transaction ends.
  An index of the parent is built concurrently on each partition, in a step of its own.
