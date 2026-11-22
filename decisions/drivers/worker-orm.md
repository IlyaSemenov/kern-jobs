# Typed table queries for simple worker mutations

The worker binds its ORM schema to its own database in the configured PostgreSQL schema.
It uses a typed table query for operations such as completing a job, where the affected row count gives the outcome.
Operations whose correctness depends on the shape of a PostgreSQL statement, such as claiming the head of a queue, use SQL on the same database.

The ORM checks column names and input types against the table definition in TypeScript.
That definition must agree with the table created by the migrations.
Hand-written SQL would avoid maintaining this second definition and compiling ORM queries at execution time.

kern-orm is installed with kern-jobs, including in producer-only applications.
Installing an incompatible version of kern-orm alongside kern-jobs requires a second copy.
Producer statements run directly on the application client, and the main entry loads no database package.
