# `maxWait` bounds the computed run time

`maxWait` of debounce bounds the `run_at` that the enqueue computes, not the time of the claim.
The claim can come later: a producer transaction that holds the due row makes claiming skip it, and so do workers without free slots, downtime, and retries.
Every claim returns its claim lag, the claim time minus `run_at`, which shows such delays.

A row that producers hold at every poll is skipped at every poll.
A deterministic test keeps a row with `run_at` 10 unclaimed for 11 ticks of 0.5 seconds; with 8 producers on 10 keys and a worker on every tick, 3–9 of about 190 runs started after `maxWait`, the latest 1.5 seconds after it.

The alternative is an extra claim pass for overdue rows that waits for their locks under a `lock_timeout`, in which the queue of the row lock would let the claim in before later producers.
It would bound the start of a job more tightly, at the cost of a statement per cycle and of worker slots that wait for producer transactions up to the timeout.
