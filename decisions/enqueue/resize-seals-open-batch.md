# Another `maxItems` seals the open batch

An enqueue of items whose `max_items` differs from that of the open batch of the key, such as after a deploy that changes `maxItems`, seals the open batch with its items and run time and appends every item to new jobs of its own `max_items`.
A change of `maxItems` thus rejects no enqueue, also while old and new versions of the application enqueue side by side, at the cost of a smaller batch at each change of the capacity between them.

## Rejecting the enqueue

The enqueue could raise `55000` and change nothing, as it does for a pending job that is not a batch, so that a change of the capacity splits no batch and shows as an error rather than passing unnoticed.
It would not keep a handler from getting more items than a lowered `maxItems`, since the open batch of the old capacity still runs with its items.
Every enqueue of another capacity would fail while the batch of its key stays open, and a rolling deploy would fail the enqueues of either version.
