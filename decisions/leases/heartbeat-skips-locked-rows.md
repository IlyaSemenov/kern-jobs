# Heartbeats skip locked rows

A heartbeat locks the rows of its claims with `SKIP LOCKED` and reports a claim whose row another transaction holds as busy, neither extended nor lost:

```sql
with owned as (
  select j.claim_id from job j join x on (j.id, j.claim_id) = (x.id, x.claim_id) where j.state = 'active'
), locked as (
  select j.id, x.lease from job j join x on (j.id, j.claim_id) = (x.id, x.claim_id) where j.state = 'active'
  for update of j skip locked
), extended as (
  update job j set lease_until = greatest(j.lease_until, now() + make_interval(secs => l.lease))
  from locked l where j.id = l.id returning j.claim_id
)
select claim_id, claim_id in (select claim_id from extended) as extended from owned
```

## Alternatives

A plain `UPDATE` waits for the locks of other transactions:

```sql
update job j set lease_until = greatest(j.lease_until, now() + make_interval(secs => x.lease))
from x where (j.id, j.claim_id) = (x.id, x.claim_id) and j.state = 'active'
returning j.claim_id
```

Its SQL is simpler, and every answer is definitive: a claim is extended or lost.
But a fail transaction holds the row of its claim while it waits for the pending job of its key, which a producer holds until its transaction ends, and one heartbeat carries every claim of the worker.
A deterministic test reproduced the cost: a heartbeat of two claims waited for as long as the producer transaction, while the fail of one of them waited for the producer, so the lease of the other claim lapsed and its handler would be aborted.

Leaving out of the heartbeat the claims whose settlement runs keeps the plain `UPDATE`.
But a heartbeat sent just before a fail transaction locks the row still waits for that transaction, and a complete that waits for a connection of a busy pool runs without heartbeats and can lose its lease.

## Cost

A heartbeat takes 1.4 to 1.6 times as long as the plain `UPDATE`, for 1 to 1000 claims.
A claim whose row the reaper holds is reported busy instead of lost until the reaper commits, so the worker learns of the loss from a later heartbeat.
