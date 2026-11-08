# Ownership by the claim, not the lease

The claim id alone decides whether a worker owns a job: a heartbeat or complete after the lease expired succeeds until the reaper returns the job.
The expiry only makes the job eligible for the reaper.

Checking `lease_until` in complete and heartbeat too would reject a late settlement even when nobody else took the job.
The finished work of a handler that overran its lease would then be lost and repeated, and a heartbeat delayed by the network would lose a job that no other worker wanted.
Neither check stops a handler that keeps running after its lease, so only the abort of the handler and the fencing of its settlement protect the next execution.
