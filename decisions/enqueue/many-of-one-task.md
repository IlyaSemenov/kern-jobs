# `enqueueMany` takes the payloads of one task

`jobs.enqueueMany` takes one task, an array of its payloads, and overrides for all of them:

```ts
await jobs.enqueueMany(indexProductTask, [{ productId: "1" }, { productId: "2" }])
```

One type parameter relates every payload to the task, the call resolves the scope of the task once, and the payloads of one task and scope tell their keys apart by the stored key alone.
The key and the options of a payload still come from `withKey` and `withOptions`.

## Items of several tasks

`jobs.enqueueMany([{ task, payload, overrides }, …])` adds the jobs of several tasks in one statement, all or none, such as the email and the invoice of one order.
It costs the types of the API, which must relate each payload to its own task, by a mapped type over a tuple of items or by a helper that builds each item, the resolution of the scope of each scoped task, and a statement that takes the task and the scope of each item and tells the rows of its result apart by them.
An application gets the same atomicity by running an enqueue statement per task in a single transaction of the application.

Graphile Worker takes jobs of several tasks in `addJobs` ([docs](https://github.com/graphile/worker/blob/v0.18.0/website/docs/library/add-jobs.md#L42-L94)), and Oban in `insert_all` ([docs](https://github.com/oban-bg/oban/blob/v2.24.2/lib/oban.ex#L792-L865)).
