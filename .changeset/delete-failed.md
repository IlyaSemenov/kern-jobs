---
"kern-jobs": minor
---

Add `jobs.deleteFailed`, which deletes failed jobs in bulk, filtered by task, by age, or both, with the scope rules of `jobs.deletePending`, without looking up their ids, and resolves to their count.
