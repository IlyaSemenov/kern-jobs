# The reaper bounds its wait and leaves the blocked row

A reaper statement sets `lock_timeout` for itself, local to its transaction, and joins the setting into its first select:

```sql
with timeout as materialized (select set_config('lock_timeout', '1000', true)),
expired as (
  select j.id, … from job j, timeout where j.state = 'active' and j.lease_until < now()
  order by j.id limit 100 for update of j skip locked
)
update job j set state = 'pending', … from expired …
```

When a producer holds an uncommitted pending row of the key of a returning row, the update waits for it in the unique check while it holds every row of its batch.
A deterministic test reproduced the cost: of two expired rows, one with the blocked key and an independent one, the independent one stayed active until the producer ended.
After the timeout the reaper redoes the batch row by row, so the blocked row cannot hold another one, and leaves a row that times out active for the next round.

## Alternatives

A timeout alone, without the row by row redo, selects the same blocked batch again in the next round.

`SET lock_timeout` on a connection needs the connection that runs the reap, which a pool does not guarantee, and a `SET LOCAL` in a transaction needs a reserved connection and two more statements for every batch.

Detaching a returning row from its key on every recovery needs no unique check, so its statement never waits for a producer, and its SQL is simpler.
But then an event of the key that arrives after the recovery, before the job is claimed again, creates a pending job of its own instead of merging into the returned one, also when no producer held the key.

Detaching a row whose wait timed out would return it at once.
But the row then loses its key also when the producer rolls back, so that later events of the key do not merge into it, and when the producer commits, its job runs next to the returned one.

## Cost

A key that a producer holds for longer than the timeout costs a round the timeout for the batch and once more for its row, and its row waits again in every round until the producer ends.
