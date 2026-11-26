# A batch job that takes no more items has `sealed` beside its state

A batch job that takes no more items, full, claimed once, or of a capacity that a later enqueue changed, has `sealed` true in every state, and the unique index of the open keys, `job_open_key_idx`, leaves sealed pending rows out, so several sealed batch jobs of a key wait pending, with their key, beside its open batch.
Claims, the summary view, and the deletes of pending jobs read a sealed pending job as any pending one.

## A state of its own

A state `sealed` beside `pending` would leave the unique index of pending keys and its `ON CONFLICT` as they were, `state = 'pending' and key is not null`, but every claim, the queue scan, the ready indexes, the summary view, the deletes of pending jobs, and the deletes of the application from SQL would have to take the new state too.

## The key in another column

A sealed batch job could move its key to another column, which leaves it outside the index of pending keys without a change of the index.
Administration and the deletes of the application from SQL find the jobs of a key by `key`, which would miss the sealed ones.
