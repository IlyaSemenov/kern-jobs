# Rate limits

A limit of the starts of a task or a queue per period, such as of `accounts.sync` against a service that answers with `Retry-After`.
No use case yet: a handler throws `RetryError` with the delay of the service instead.
