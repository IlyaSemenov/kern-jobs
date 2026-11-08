# A transaction owns its connection

Fail runs its transaction on a connection that the driver reserves from `BEGIN` to the end of the transaction, while claims and single-statement settlements run on any connection of the pool.

The SQL spike sent all statements of a worker through one connection, whose client queued them, so a worker needed one connection besides its heartbeats.
A statement of another handler, sent while a fail transaction waited between its statements, then ran inside that transaction and rolled back with it.
Deterministic tests reproduced both cases on the shared connection: a complete reported success while its job stayed active, and a claim cycle claimed a job whose claim then rolled back, so another worker could claim it again while the first handler ran it.
With a reserved connection, both stay applied.

The cost is a pool instead of one connection per worker; a fail transaction holds its connection only for its few statements, since the worker computes the backoff before it starts.
