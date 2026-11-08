# kern-jobs

Background jobs on PostgreSQL for TypeScript: typed tasks, transactional enqueue, keys, debounce, queues, and leases.

See the [documentation](https://ilyasemenov.github.io/kern-jobs/).

## Install

```sh
npm install kern-jobs postgres
```

## Usage

```ts
import { createJobs, defineTask } from "kern-jobs"
import postgres from "postgres"

const sendEmailTask = defineTask("email.send").withPayload<{ to: string }>()

const sql = postgres(process.env.DATABASE_URL!)

const jobs = createJobs(sql)

await jobs.enqueue(sendEmailTask, { to: "ann@example.com" })
```
