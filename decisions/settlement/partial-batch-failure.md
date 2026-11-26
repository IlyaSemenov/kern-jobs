# A batch handler reports its failed items by a thrown error, recorded as text

A handler of a batch task fails some of its items by throwing a `BatchItemsError` of their indexes and errors, and the job keeps only those items, in its own row, with its attempts and backoff.
The fail records the error of the items in `last_error` as text: a line of the counts and the attempt, and a line of the index and the message of each item.

## A returned result of each item

Graphile Worker takes a returned array of a promise of each item, and fails the job with the items whose promises rejected.
A result needs no error class, and a handler that maps its items to promises returns them as they are.

A handler that returns completes its job, and a `runHandler` that resolves completes it too, also one that awaits `run` without returning its result: such a wrapper would complete a batch whose items failed.
A thrown error keeps that contract, reaches `onError` and the decision of the worker as `TerminalError` and `RetryError` do, and leaves the return type of a handler as it is.

## Structured errors of the items

A column of the errors of the items, such as `item_errors jsonb`, beside `last_error`, would give `onFailure` an error of each item of its payload, and an operator a query of them, without parsing the text.
It needs a migration, a statement of the fail that writes the column, and a field of `JobFailure`, and the text gives an operator the same index and message.
