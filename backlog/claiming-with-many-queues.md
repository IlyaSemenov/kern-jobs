# Claiming with many queues

[Keys](../docs/tasks/keys.md#while-a-job-of-the-key-runs) suggests a queue per entity beside its key to run the jobs of an entity one at a time, which gives an application as many queues as entities, such as 100,000 queues of one or two jobs each.
The queue scan of `claimQueued` visits the queues in order from the cursor of the worker until it finds a free one, so its cost grows with the busy queues before the first free one.
`queueBacklog` of `src/core/core.bench.ts` measures `claimQueued` over 100,000 ready rows in a given number of queues, with all of them free and with one of them busy, and the claim cycles of 8 workers of 4 slots over them.

- Measure `claimQueued` and the claim cycle with many queues, most of them busy, and with many queues of one ready job each.
- Open question: past which count of busy queues the claim slows down, and whether the scan should skip busy queues by the active rows rather than visit them.
- Once measured, keep or change the advice of a queue per entity in Keys.
