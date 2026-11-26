# Partial failures of batches

A `BatchItemsError` retries its failed items after the backoff of the task, and fails them for good only on the last attempt of the job; it lacks:

- An item that fails for good at once, such as by a `TerminalError` of the item, while the other failed items retry: one settlement would make a failed row of those items, with its failure job, and return a pending row of the others.
- One delay of the retry of all the failed items, as `RetryError` gives a job.
- The errors of the items as data, for `onFailure` and the queries of an operator, beside the text of `last_error` ([decision](../decisions/settlement/partial-batch-failure.md#structured-errors-of-the-items)).
