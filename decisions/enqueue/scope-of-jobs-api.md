# A task is scoped, and the jobs API gives the scope

A task declares that each of its jobs belongs to a scope, such as of a tenant, and `createJobs` gives the value, a string or a function that each enqueue of a scoped task calls, such as one that reads the tenant of the current request:

```ts
const indexProductTask = defineTask("search.indexProduct", { scoped: true }).withPayload<{
  productId: string
}>()

const jobs = createJobs(sql, { scope: () => currentTenantId() })

await jobs.enqueue(indexProductTask, { productId })
```

An enqueue of a scoped task without a scope throws, and the jobs of other tasks have no scope whatever the jobs API gives, so neither kind of task lands in the scope of the other by a jobs API created for the wrong one.

## A scope of the jobs API only

`createJobs(sql, { scope: tenantId })` without a setting of the task is one option less, and an application creates the jobs API of a tenant per request, which costs no I/O.
But it ties the scope to the code that enqueues rather than to the task: a task of a tenant enqueued through a jobs API without a scope merges with the jobs of other tenants without an error, and a task of all tenants, such as a cron job, enqueued through the jobs API of a tenant splits its key by tenant.
An application also passes the jobs API of the tenant to each place that enqueues, or reads the tenant of the request at each of them, while the function of the scope reads it in one place, at each enqueue of a scoped task.

## A scope among the options of an enqueue

A `scope` that `withOptions` computes from the payload or an override passes puts the tenant in each payload or each call, and a task or a call that forgets it enqueues into the empty scope.

## A wrapped definition in the application

graphile-worker-tasks 2.5.0 wraps the definition of a tenant task in the application: its enqueue reads the tenant from the context of the application, stores it in the payload, prefixes the key and the queue with it, and its handler runs in the context of the tenant ([docs](https://github.com/IlyaSemenov/graphile-worker-tasks/blob/v2.5.0/README.md#wrapping-tasks)).
It needs nothing of the queue, but the prefixes and the payload envelope are the work of the application, which the column `scope` of kern-jobs does, and the context of a handler stays a hook that kern-jobs lacks ([Backlog](../../backlog/scopes-in-the-application-context.md)).
