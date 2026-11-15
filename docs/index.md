---
layout: home

hero:
  name: kern-jobs
  text: Background jobs for TypeScript, stored in PostgreSQL
  tagline: Enqueue work in the transaction that writes your data, share typed task definitions with your workers, and control retries and repeated updates.
  actions:
    - theme: brand
      text: Get Started
      link: /getting-started
    - theme: alt
      text: Compare
      link: /comparison

features:
  - title: Commit data and jobs together
    details: Workers see a job once the transaction that enqueued it commits, and never when it rolls back.
  - title: Define tasks once
    details: Your application and its workers share the payload type of each task, and its handler stays in the worker.
  - title: Control repeated work
    details: Merge the pending jobs of one entity, debounce bursts of updates, and run the jobs of an entity one at a time.
  - title: Recover from failures
    details: Retry failed jobs with backoff, run again the jobs of a worker that dies, and stop workers without losing their jobs.
---

Enqueue a job in the transaction that writes its data:

```ts
await sql.begin(async tx => {
  await tx`update product set price = ${price} where id = ${productId}`
  await createJobs(tx).enqueue(indexProductTask, { productId })
})
```

kern-jobs works with your PostgreSQL client: postgres.js, node-postgres, Orchid ORM, kern-orm, kern-db, or [another client](./database-clients#other-clients) that runs SQL text with parameters.
See [Database Clients and Transactions](./database-clients) to enqueue in the transactions of each of them.

kern-jobs is part of [Kern](https://ilyasemenov.github.io/kern-orm/ecosystem), a family of PostgreSQL libraries for TypeScript, and integrates with [kern-orm and kern-db](./database-clients#kern-orm-and-kern-db) when your application uses them.
