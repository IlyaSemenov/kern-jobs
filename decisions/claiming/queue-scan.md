# Queue scan by a loose index scan

The claim of a queue finds the distinct `(scope, queue)` of pending rows with a recursive query that jumps from one queue to the next through the index, and selects the head of a queue with `LATERAL … ORDER BY run_at, id LIMIT 1`.
Its cost grows with the number of queues it visits, not with their backlog.

`SELECT DISTINCT scope, queue` is simpler, but reads every ready row of the busy queues.
With 100,000 ready rows in three queues, it took about 30 times longer than the loose index scan, which took the same time with 1,000 queues.

Joining the head to the selected queue without `LATERAL` let the planner choose a bitmap scan and sort all 33,000 rows of the queue.
