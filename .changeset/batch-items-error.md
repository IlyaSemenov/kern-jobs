---
"kern-jobs": minor
---

Add `BatchItemsError`, which a handler of a batch task throws with the indexes of its failed items, so that the job retries and fails for good with only those items.
