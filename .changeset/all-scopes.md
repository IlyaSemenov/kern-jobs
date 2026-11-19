---
"kern-jobs": minor
---

`retry`, `delete`, and `deletePending` without a filter, through `jobs` created without a scope, act only on unscoped jobs rather than on the jobs of every scope.
Pass `scope: ALL_SCOPES` to `createJobs` to act on the jobs of every scope.
