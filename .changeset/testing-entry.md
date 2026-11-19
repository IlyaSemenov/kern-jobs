---
"kern-jobs": minor
---

Add `kern-jobs/testing` with `createTestJobs`, a jobs API for tests that records enqueues in memory, and `runJob`, which calls the handler of a task directly with its payload through JSON and the metadata of its job.
