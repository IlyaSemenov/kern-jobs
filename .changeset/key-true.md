---
"kern-jobs": minor
---

Accept `key: true` as the empty key in the options of an enqueue and the filter of `deletePending`, so that the enqueues of a task merge into one pending job per scope without a constant key.
