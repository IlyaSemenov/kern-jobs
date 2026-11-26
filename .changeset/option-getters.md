---
"kern-jobs": patch
---

Fix enqueues, `defineTask`, and schedules ignoring options that are inherited or given by getters, such as an inherited `key`.
Options that are `null`, an array, or another value that is not an object throw `TypeError` when passed to `defineTask`, given as the overrides of an enqueue or a schedule, or returned by the function of `withOptions`.
