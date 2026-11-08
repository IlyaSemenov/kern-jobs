# A busy queue head skips the queue

The claim of a queue selects its head without a lock and then locks only the head with `FOR UPDATE SKIP LOCKED`.
When another transaction, usually a producer, holds the head, the claim returns `busy`, and the cycle skips the queue until its end, so the next job of the queue does not start before the head.
The stress test of 8 workers counted 320–470 such skips per run.

Waiting for the lock of the head with `FOR UPDATE` also keeps the order and claims the head as soon as the producer commits.
But a producer that enqueues in its transaction holds the head until that transaction ends, and the worker would wait for the whole transaction of the application.

`SKIP LOCKED` over all jobs of the queue never waits, but starts the next job while a producer holds the head, which breaks the start order.
