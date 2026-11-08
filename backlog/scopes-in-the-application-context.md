# Scopes in the application context

A hook of the worker that runs the handler of a scoped task in the context of its scope, such as of a tenant, instead of a handler that opens the data of `job.scope` itself, and skips the jobs of a scope that no longer exists, such as of a deleted tenant.
