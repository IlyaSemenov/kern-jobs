# Testing helpers

Capturing the enqueues of a test, reading pending jobs in the transaction of the test, and running a handler with given attempt metadata.
Also running the pending jobs inside the transaction of a test, so that their handlers see its data, and filtering the captured jobs by scope.

graphile-worker-orchid has a test double of its utilities that keeps the pending jobs of a test in memory and updates the pending job with the same key by the `jobKeyMode` of an enqueue, without a database ([docs](https://github.com/IlyaSemenov/graphile-worker-orchid/blob/v2.5.1/README.md#testing)).
