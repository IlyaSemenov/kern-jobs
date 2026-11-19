---
"kern-jobs": minor
---

Add `force: true` to `worker.stop`, which aborts the handlers and rejects at once, also when another call has begun the stop; every call of `stop` now checks its options.
