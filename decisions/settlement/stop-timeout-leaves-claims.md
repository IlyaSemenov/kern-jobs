# At its timeout, stop leaves the jobs of running handlers to their leases

At the timeout of `stop`, the worker stops heartbeating, starts no operation, and rejects; a handler that still runs keeps running, and its job returns after its lease with its attempt spent, as after a crash.
Neither the worker nor kern-db can cancel a statement in flight or stop JavaScript that ignores its signal.

## Alternatives

Failing the jobs of the handlers that still run at once, as `stop` of pg-boss does at its timeout and as the forceful shutdown of Graphile Worker does, returns them before their leases end.
But their handlers may still run, so the next execution overlaps them from the timeout on rather than from the end of the lease, and the fail spends an attempt, failing for good a job on its last attempt.

Requeuing them at once with the attempt given back also returns them sooner, with the same overlap, and a handler that hangs on every run would return its job forever without spending an attempt.

Heartbeating the jobs of such handlers until they end avoids the overlap, but a handler that never ends then holds its job forever, and the worker never stops.

Leaving the jobs to their leases gives up the faster return of these jobs: they wait for their leases to expire, which a heartbeat sent before the timeout may extend once more.
