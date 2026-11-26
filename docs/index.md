---
layout: home

hero:
  name: kern-jobs
  text: Background jobs for TypeScript, stored in PostgreSQL
  tagline: Enqueue typed jobs in the same transaction as your data changes, using your own client or ORM. Merge repeated enqueues, debounce bursts of updates, and run jobs one at a time per entity.
  actions:
    - theme: brand
      text: Get Started
      link: /getting-started
    - theme: alt
      text: Compare
      link: /comparison

features:
  - title: Commit data and jobs together
    details: Workers see a job only after its transaction commits.
  - title: Define tasks once
    details: Share typed task definitions with your workers, without importing their handlers into your application.
  - title: Control repeated work
    details: Merge repeated enqueues by key, debounce bursts of updates, and run one job at a time per entity.
  - title: Batch items
    details: Batch items from separate enqueues and set a maximum batch size.
  - title: Schedule recurring jobs
    details: Enqueue jobs on a cron schedule without duplicates across workers.
  - title: Scope jobs
    details: Separate keys and queues by scope, such as a tenant, and read the scope of a job in its handler.
  - title: Recover from failures
    details: Retry failures with backoff, recover jobs after a crash, and shut workers down gracefully.
  - title: Test without a worker
    details: Record enqueued jobs in memory and call handlers directly in tests.
---

Enqueue a welcome email in the transaction that signs up a user:

```ts
await sql.begin(async tx => {
  const [user] = await tx`insert into users (email) values (${email}) returning id`
  await createJobs(tx).enqueue(sendWelcomeEmailTask, { userId: user.id })
})
```

kern-jobs works with [your PostgreSQL client](./database-clients): postgres.js, node-postgres, Orchid ORM, kern-orm, kern-db, or another client that runs parameterized SQL.

kern-jobs is part of [Kern](https://ilyasemenov.github.io/kern-orm/ecosystem), a family of PostgreSQL libraries for TypeScript.
