# The worker retries a settlement after connection loss

After a connection error, the worker retries a complete or fail on a new connection while it can confirm the lease of the claim, and the job keeps its slot until then.
A retry is safe, since a settlement matches the claim id: it either applies the settlement or finds the claim gone, also after a statement whose outcome is unknown.

## Alternative

The worker leaves the job to the reaper: the slot frees at once, and the worker needs no retry loop.
But a job whose complete did not reach the server runs again after its lease, and a job whose fail did not apply loses its backoff and its error, since the reaper returns it with `run_at` now and `lease expired`.

In the restart scenario under load, 300 jobs of 3 workers with 2 slots each, without retries 6 of 300 jobs started twice after `pg_ctl restart` in both fast and immediate modes; with retries, none did, and the workers retried 12 to 16 settlements.
