# `keep` locks the pending row

An enqueue in `keep` mode updates the existing pending row by `ON CONFLICT DO UPDATE`, which keeps its payload, run time, queue, and attempt budget and sets only its key mode, so the row stays locked until the producer commits.
Claiming skips locked rows, so the job cannot start before the transaction of the event commits.

`ON CONFLICT DO NOTHING` neither locks the row nor writes a row version.
With it, a worker can claim the job while the transaction of the event is still open, the handler does not see the data of the event, and nothing triggers the job again: the event is lost.
The randomized test found such lost events in several of 20 rounds with `DO NOTHING`, and three deterministic tests fail with it.

The cost of the lock is a new row version per event.
On one hot key, 20,000 enqueues from 8 connections in `keep` mode ran at the rate of `replace`, within 10% either way across runs, and vacuum reclaimed their dead row versions without growing the table further.

`ON CONFLICT DO UPDATE … WHERE false` also locks the row and writes no row version, but returns no row, so the enqueue would not learn the id of the job.
