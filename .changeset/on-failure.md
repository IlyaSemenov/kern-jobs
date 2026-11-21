---
"kern-jobs": minor
---

Add `onFailure` to `task.implement`, which handles a job that fails for good in a retryable job of its own; task names ending in `:failure` are reserved for these jobs.
