# A job interrupted by stop returns without spending its attempt

A handler whose signal `stop` aborted, and that then throws any error other than a `TerminalError` or a `RetryError`, is interrupted: the worker does not report the error and, before the timeout of `stop`, requeues its job at once with the attempt given back and its `last_error` kept.
A claim that returns after `stop` began and before its timeout is requeued the same way without running its handler.
A `TerminalError` or a `RetryError` keeps its meaning after the abort, since the handler decided the outcome.

## Alternatives

Failing an interrupted job like any other error, as Graphile Worker does, keeps one rule for every error, but a deploy spends an attempt of every running job, delays it by its backoff, fails for good a job on its last attempt, and reports each abort as an error.
A job of `maxAttempts: 1` whose handler honors the abort became `failed` with `handler aborted: stop`.

Counting as interrupted only the reason of the signal, the `HandlerAbort` itself, keeps a real failure thrown after the abort a failure that is reported and spends its attempt.
But libraries wrap an abort in errors of their own, such as `setTimeout` of `node:timers/promises`, which rejects with an `AbortError` whose `cause` is the reason, so such a handler would still fail jobs for good at a deploy.

The chosen rule gives up that a real error thrown after the abort is reported and counted: such a job runs again without spending its attempt on every stop that interrupts it, beyond `maxAttempts`.

## Late claims

A claim that returns after the deadline of its lease, without a stop, spends its attempt instead: the worker fails the job without running its handler.
Giving the attempt back there would let a claim statement that is always that slow return the job forever.
