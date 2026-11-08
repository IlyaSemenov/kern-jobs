# Heartbeats on a connection of their own

A worker sends its heartbeats on a connection that nothing else uses, rather than on the pool of its claims and settlements.

Heartbeats on the pool would need no extra connection.
But a fail transaction can wait for a row lock of a producer for as long as the transaction of the producer lasts, and claims and settlements of many handlers can take every connection of the pool, so heartbeats would queue behind them and leases would expire while their handlers still run.

A worker with a broken heartbeat connection stops claiming until the connection works again: it could not keep a new lease, and every job it claimed would lose its lease and spend an attempt.
