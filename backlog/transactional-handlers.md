# Transactional handlers

A handler whose writes commit together with the completion of its job, in a transaction that the application opens, so that a crash between them cannot repeat the writes.

Preliminary: it is an option of the task.
Open: such a handler holds a connection of the pool until its job ends, so its lease may need a bound.

With `transactional`, pg-boss opens a transaction for a handler and the completion of its jobs.
It sends the heartbeats of a job with `heartbeatSeconds` from another connection.
Its `transactionTimeoutSeconds` bounds the whole transaction on PostgreSQL 17 and later, and only the pauses between the statements of the handler on earlier versions ([docs](https://github.com/timgit/pg-boss/blob/12.38.1/docs/api/workers.md?plain=1#L54-L94)).
