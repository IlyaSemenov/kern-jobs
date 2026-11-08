# Shared heartbeat connection

Every worker reserves one connection of its pool for its heartbeats, so a pool of N connections stops N workers without an error, and an application that starts a worker for each group of tasks pays a connection for each.

- A candidate is one heartbeat for the workers of a process on one driver and schema: one connection and one statement with the claims of all of them, at the shortest of their intervals, released when the last of them stops.
- Rejecting a pool too small for its workers at start, without waiting for a connection, needs the size of the pool from the driver, which the contract of the drivers of kern-db does not give; `ready` only confirms a connection beside the one of the heartbeat once a claim statement has used it.
