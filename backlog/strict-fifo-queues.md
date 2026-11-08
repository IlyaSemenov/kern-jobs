# Strict FIFO queues

A queue mode in which the head must complete before the next job starts, and a failed head blocks the queue until it is retried or cancelled.

The queue policy `key_strict_fifo` of pg-boss holds back the jobs of a key while a job of the key runs, waits for a retry, or has failed ([docs](https://github.com/timgit/pg-boss/blob/12.38.1/docs/api/queues.md?plain=1#L43)).
