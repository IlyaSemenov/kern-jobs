# The scope of the jobs API limits administration

With a `scope` of `createJobs`, retry, delete, and, without a task, `deletePending` and `deleteFailed` act only on the jobs of that scope, and call its function at each call.
Without one, they act only on unscoped jobs, and with `ALL_SCOPES`, on the jobs of every scope:

```ts
const tenantJobs = createJobs(sql, { scope: tenant.id }) // the jobs of the tenant
const unscopedJobs = createJobs(sql) // unscoped jobs
const allJobs = createJobs(sql, { scope: ALL_SCOPES }) // the jobs of every scope
```

A jobs API of one scope, such as of the current request, then never changes a job of another scope by id or without a filter, and one that misses its scope never reaches the jobs of a tenant, though a scope is no access control.

## Ignoring the scope

Ignoring the scope, so that retry and delete find a job by its id in any scope, lets the one jobs API of an application, whose scope function reads the current request, serve scripts of operators outside a request, and reach the jobs of tasks that are not scoped, which it enqueues too.
But a call through the jobs API of one scope would then change a job of another scope whose id it was given.

## Every scope without a scope

A jobs API without a scope could act on the jobs of every scope instead.
It needs no exported value such as `ALL_SCOPES`, and a script of an operator creates it as `createJobs(sql)`.
But a jobs API that misses its scope by mistake, such as `createJobs(sql)` in a handler instead of `createJobs(sql, { scope: job.scope })`, then retries or deletes a job of a tenant by its id, and deletes the pending jobs of every tenant by `deletePending()`, without an error, while an enqueue of a scoped task through it throws.

## Every scope by an option of a call

`jobs.deletePending({ task, scope: ALL_SCOPES })` lets the one jobs API of an application, whose scope function reads the current request, act on every scope without a second jobs API.
But a call through the jobs API of one scope would then change the jobs of other scopes, and a `scope` of a call would be a second place to give the scope beside `createJobs`.
