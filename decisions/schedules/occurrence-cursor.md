# A durable cursor for each schedule

Advance an inclusive minute cursor in the transaction that enqueues a job or merges it into a pending job.
One row per schedule records how far evaluation reached, including skipped minutes.
It stays in the database when the job completes or is deleted.

## A key on the pending job

Using a job key would save the schedule row.
Successful completion deletes the job, so a replica checking the same minute could enqueue it again.

## A row per occurrence

A ledger could record the outcome of each occurrence for auditing.
Its storage grows with the number of occurrences, while deduplication needs only the last evaluated minute.
