# Accumulating payloads

Batch tasks of `withBatchPayload`, which append the items of each enqueue to the open batch of their key, lack:

- An option `distinct` that skips an item that the batch has, also of an earlier enqueue, which `maxItems` would then count once; the batch keeps duplicates.
- A table of items for batches of thousands of items: each append rewrites the whole payload, a new row version with its TOAST, which `maxItems` bounds.
