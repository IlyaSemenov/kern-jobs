# Metrics read a summary view

The migrations create the view `job_summary`, with counts of jobs by state, ages, and attempts for each task in each scope, and its columns are the contract that metrics of an application read.
The application does not interpret the columns of the table, such as which pending jobs wait for a backoff, and can grant its monitoring role `SELECT` on the stable columns of the view rather than columns of the table that a migration may change.
A change of the table, such as of its placement or its states, keeps the view, which only gains columns.

A documented contract of the columns of the table instead would let an application count any slice of the jobs without waiting for a release of kern-jobs, but each such column would bind a later migration of the table, and the meaning of a state, such as of a retry, would stay with each application.
A method of `jobs` that reads the counts would serve an application that exports metrics from JavaScript, but not a SQL exporter, which reads the database itself.
