# Getting Started

::: warning
kern-jobs is in early development, and its API changes between minor versions.
:::

## Install

::: code-group

```sh [npm]
npm install kern-jobs postgres
```

```sh [pnpm]
pnpm add kern-jobs postgres
```

```sh [bun]
bun add kern-jobs postgres
```

:::

Install the database client of your application beside kern-jobs: postgres.js, as in this guide, or another one, such as node-postgres.
kern-jobs connects through it in two ways:

- The code that enqueues jobs passes `createJobs` the client itself, such as `sql`, or the client of a transaction, so that its enqueues join that transaction.
- `migrateJobs` and the worker take a driver that wraps the client, such as `postgresJsDriver(sql)`, and the worker runs outside the transactions of your application.

See [Database Clients](./database-clients) for each client.

kern-jobs runs on Node.js 24 or newer and Bun 1.4 or newer, with PostgreSQL 15 or newer.
Use TypeScript 6.0 or newer.

## Try it

To see a job run from start to end, create a project in an empty directory:

```sh
npm init -y
npm pkg set type=module
npm install kern-jobs postgres
```

Save this module in it:

<<< ../tests/package/quickstart/quickstart.ts [quickstart.ts]

Run it with the URL of a PostgreSQL database, such as:

::: code-group

```sh [Node.js]
DATABASE_URL=postgres://localhost:5432/postgres node quickstart.ts
```

```sh [Bun]
DATABASE_URL=postgres://localhost:5432/postgres bun quickstart.ts
```

:::

It creates the schema of kern-jobs in the database, enqueues a job, runs it in a worker, which prints `Hello, world!`, and stops the worker.

The sections below split it into the modules of an application.

## Create the schema

kern-jobs keeps its jobs in a PostgreSQL schema of its own, `kern_jobs`.
Create it with `migrateJobs` before your application starts, such as in the script that applies its migrations:

```ts [src/migrate.ts]
import { postgresJsDriver } from "kern-jobs/db/postgres-js"
import { migrateJobs } from "kern-jobs/migrations"
import postgres from "postgres"

const sql = postgres(process.env.DATABASE_URL!)

await migrateJobs(postgresJsDriver(sql))
await sql.end()
```

Run it at every deploy as well, as [Updating kern-jobs](./deployment/updating-kern-jobs) describes.

To use a different schema name, pass it as `schemaName` to `migrateJobs`, and to `createJobs` and `startWorker` below.

With kern-orm or kern-db, you can pass `migrateJobs` your `db` or `database` instead of a driver, so that it creates the schema in the current transaction, such as a test transaction that rolls it back.

## Define tasks

A task is a kind of work, such as sending an email.
Define each task with its name and the type of its payload in a module that both the code that enqueues jobs and the worker import:

```ts [src/tasks.ts]
import { defineTask } from "kern-jobs"

export const sendWelcomeEmailTask = defineTask("email.sendWelcome").withPayload<{
  userId: string
}>()
```

[Defining Tasks](./tasks/) describes the options of a task, such as delays, and the pages after it describe keys, queues, and scopes.

## Connect

Create the jobs API of your application with the database client to enqueue through:

```ts [src/jobs.ts]
import { createJobs } from "kern-jobs"
import postgres from "postgres"

export const sql = postgres(process.env.DATABASE_URL!)

export const jobs = createJobs(sql)
```

[Database Clients](./database-clients) shows how to connect node-postgres and other clients, and how to enqueue in the transaction of your application.

## Enqueue

Enqueue a job of a task with its payload:

```ts
await jobs.enqueue(sendWelcomeEmailTask, { userId: user.id })
```

An enqueue in a transaction of your application becomes visible to workers when the transaction commits and never runs when it rolls back, so a job never misses the data that the transaction writes.

## Run a worker

Implement each task with the handler of its jobs, as the default export of a module that only the worker imports:

```ts [src/worker/tasks/send-welcome-email.ts]
import { sendWelcomeEmail } from "../../email"
import { sendWelcomeEmailTask } from "../../tasks"

export default sendWelcomeEmailTask.implement(async ({ userId }) => {
  await sendWelcomeEmail(userId)
})
```

List the implementations of your tasks in a manifest:

```ts [src/worker/manifest.ts]
import { defineManifest } from "kern-jobs/worker"

import sendWelcomeEmail from "./tasks/send-welcome-email"

export const manifest = defineManifest({ tasks: [sendWelcomeEmail] })
```

Start a worker with the manifest in a process of its own:

```ts [src/worker/start.ts]
import { postgresJsDriver } from "kern-jobs/db/postgres-js"
import { startWorker, stopOnSignals } from "kern-jobs/worker"

import { sql } from "../jobs"
import { manifest } from "./manifest"

const worker = startWorker(postgresJsDriver(sql), { manifest })

stopOnSignals(worker, { close: () => sql.end() })
```

On SIGTERM or SIGINT, `stopOnSignals` stops the worker, closes the database client, and exits the process.
In a process that also serves your application, [call `stop`](./workers/stopping#stop-from-your-code) from its shutdown instead.

A job whose handler returns is done, and a job whose handler throws runs again later.
[Running Workers](./workers/) and the pages after it describe handlers, retries, leases, stopping, and monitoring.
