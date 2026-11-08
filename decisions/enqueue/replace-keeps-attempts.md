# `replace` keeps attempts

An enqueue in `replace` mode keeps `attempts` of the pending row, also while the row waits for a retry; the producer resets them explicitly.
The row is the unit of the retry budget, so a stream of events cannot keep a job that always fails retrying forever: with a budget of three attempts, three failures make the job `failed` even when `replace` events arrive between them.

Graphile Worker resets attempts on every replace.
That gives a new event a full budget, which suits a job whose failures came from data that the new event fixes, at the cost of retrying a poisoned job as long as events arrive.
