---
"kern-jobs": minor
---

Add batch tasks, defined by `defineTask(…).withBatchPayload<Item>({ maxItems })`, whose jobs run with up to `maxItems` items: `jobs.enqueueItems(task, items)` and the SQL function `enqueue_items` append items to the pending job of their key.
Stop the producers and the workers of the previous version before `migrateJobs`: their enqueues fail on the updated schema.
