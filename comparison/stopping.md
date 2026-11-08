# Stopping

## A job interrupted by stop

`stop` takes no more jobs and lets the running handlers end until `abortAfter`, 5 seconds by default, then aborts their signals.
A job whose handler throws after the abort, other than a `TerminalError` or a `RetryError`, returns to pending at once without spending its attempt, also on its last attempt.

- Graphile Worker, differs: it stops taking jobs at a termination signal, waits for its running jobs, and aborts their signal after `gracefulShutdownAbortTimeout`, 5 seconds by default ([docs](https://github.com/graphile/worker/blob/v0.18.0/website/docs/error-handling.md#termination-signal), [config](https://github.com/graphile/worker/blob/v0.18.0/website/docs/config.md#workergracefulshutdownaborttimeout)); a job whose handler then throws fails like any other and spends its attempt.
- pg-boss, differs: `stop` waits for the active handlers up to `timeout`, 30 seconds by default, and then fails and aborts their jobs ([docs](https://github.com/timgit/pg-boss/blob/12.38.1/docs/api/ops.md#stopoptions)).

## A handler that does not end by the timeout of stop

At `timeout`, `stop` rejects and stops heartbeating; a handler that still runs keeps running, and its job returns after its lease with its attempt spent.

- Graphile Worker, differs: it ignores further termination signals for 5 seconds, and a signal after that starts a forceful shutdown, which fails the jobs of the handlers that still run and kills the process ([source](https://github.com/graphile/worker/blob/4cda192c5df254392a1dff350e5d73f7d2c18a85/src/main.ts)).
- pg-boss, differs: at its `timeout`, `stop` fails the jobs of the handlers that still run ([docs](https://github.com/timgit/pg-boss/blob/12.38.1/docs/api/ops.md#stopoptions)).
