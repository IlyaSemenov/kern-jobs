# Accumulating payloads

A key mode `append` that appends the payload of each event to the array of the pending job.

- While the job of the key is active, the items go to the next pending job.
- An option `distinct` skips an item that the array has, and `maxItems` runs a full job at once and starts the next one with the later items; both work with `debounce`.
- Each append rewrites the whole payload, a new row version with its TOAST, so `maxItems` is required, and thousands of items need a table of items instead.
- The check constraint of `key_mode` and the enqueue statement must accept the new mode.

Graphile Worker concatenates array payloads when an enqueue updates the pending job of its key ([docs](https://github.com/graphile/worker/blob/v0.18.0/website/docs/job-key.md#job_key_mode)).
