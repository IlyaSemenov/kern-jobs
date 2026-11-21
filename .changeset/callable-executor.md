---
"kern-jobs": patch
---

Fix `createJobs` calling a callable executor as a client factory instead of running statements through its `query` method.
