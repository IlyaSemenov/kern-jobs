# Testing

To check which jobs your code enqueues without a database, give it the jobs API of `createTestJobs` from `kern-jobs/testing` in tests, in place of the one of `createJobs`.
It records each enqueue in memory:

```ts [src/testing/jobs.ts]
import { createTestJobs } from "kern-jobs/testing"

export const testJobs = createTestJobs()
```

Make the code under test take its jobs API from a place that a test can replace, such as a parameter or a dependency injection container, and pass it `testJobs`.

## Check enqueued jobs

Run the code under test in `capture`, and query the jobs that it enqueued:

```ts [src/accounts.test.ts]
import { expect, test } from "bun:test"

import { signUp } from "./accounts"
import { sendWelcomeEmailTask } from "./tasks"
import { testJobs } from "./testing/jobs"

test("enqueues a welcome email for the new user", async () => {
  const captured = await testJobs.capture(() => signUp(testJobs, "user@example.com"))

  expect(captured.payloadsFor(sendWelcomeEmailTask)).toEqual([{ userId: captured.result.id }])
})
```

- The queries cover every job enqueued through `testJobs` while `capture` runs.
  Finish the setup of fixtures before you start `capture`, and await it before the next step, so that their jobs stay out.
- `captured.result` is the value that the action returns, such as the user that `signUp` creates.
- `captured.callsFor(task)` gives the scope, the payload, and the options of each enqueue of the task.
  The options are those of `defineTask`, of `withOptions`, and of the enqueue, merged as an enqueue merges them, so check a key or a delay there; `key: true` is recorded as the empty key, `""`.
- `testJobs.calls` lists every recorded enqueue, and `testJobs.clear()` forgets them, such as before each test.
  `enqueueMany` records an enqueue of each of its payloads.

An enqueue through `testJobs` rejects invalid options, a payload that `JSON.stringify` throws for, such as a bigint, and a scoped task without a scope as `createJobs` does, and records nothing then; `enqueueMany` also rejects two payloads of one key, and records none of its payloads when it rejects.
If a pending job has the same task name, scope, and key, `enqueue` resolves to `{ id, created: false }` with its id, and `enqueueMany` gives such an object for the payload.
Its jobs stay pending, since no worker runs them: `deletePending` and `delete` delete them, and nothing fails, so `retry` resolves to `null`.
Each `createTestJobs()` keeps its own jobs, so another one, also with `ALL_SCOPES`, does not reach them.

`testJobs` ignores transactions: an enqueue in a transaction that rolls back stays recorded.
Test what a rollback does with `createJobs` on a database, and also the limits of the table, such as a key of at most 1024 bytes, which `testJobs` does not check.

## Scoped tasks

Pass `createTestJobs` the scope of [scoped tasks](../tasks/scopes) as you pass it to `createJobs`, such as a function that returns the tenant of the current test:

```ts [src/testing/jobs.ts]
import { createTestJobs } from "kern-jobs/testing"

import { currentTenantId } from "../tenants"

export const testJobs = createTestJobs({ scope: () => currentTenantId() })
```

`callsFor` and `payloadsFor` of a scoped task return only the jobs of the scope that the function returns when you call them, so a job enqueued for another tenant fails the check.
For a task that is not scoped, they return its unscoped jobs.

## Run a handler

To test a handler, run its implementation with a payload by `runJob`:

```ts [src/worker/tasks/send-welcome-email.test.ts]
import { runJob } from "kern-jobs/testing"

import sendWelcomeEmail from "./send-welcome-email"

await runJob(sendWelcomeEmail, { userId: "user-1" })
```

`runJob` resolves to what the handler returns, or rejects with what it throws.
To run the handler through the [`runHandler`](../workers/handlers#wrap-every-handler) of your worker, pass it in `runHandler`: `runJob` then resolves or rejects as `runHandler` does.
The handler gets the payload as a worker reads it from the database, through JSON, so a `Date` in the payload reaches it as a string.
Its `job` has the key, the queue, and `maxAttempts` that the options of the task give for the payload, with `""` for `key: true`, and the first attempt.
Replace them in `job`, such as to test the last attempt:

```ts
await runJob(sendWelcomeEmail, { userId: "user-1" }, { job: { attempt: 25 } })
```

Pass an `AbortSignal` in `signal` to test how the handler [stops](../workers/handlers#stop-long-handlers).

`runJob` calls the handler without [retries](../workers/retries) or `onFailure`; to test `onFailure`, run it by [`runFailure`](../workers/failure-handlers#test-onfailure).

For a [scoped task](../tasks/scopes), pass the scope of its job to `runJob` in `job.scope`.
To give its job the scope of `testJobs` instead, call `testJobs.runJob` without `job.scope`:

```ts [src/worker/tasks/build-report.test.ts]
import { testJobs } from "../../testing/jobs"

import buildReport from "./build-report"

await testJobs.runJob(buildReport, { reportId: "report-1" })
```

Its job gets the scope that the `scope` of `createTestJobs` gives at the call, such as the tenant of the current test; a job of a task that is not scoped gets none.
Pass `job.scope` to `testJobs.runJob` to give the job another scope.

## Check the manifest

A job of a task that no worker implements stays pending without an error.
To catch a task that you defined and forgot to implement, compare the tasks of your application with its manifests in a test:

```ts
import { findUnimplementedTasks } from "kern-jobs/worker"

import * as tasks from "../src/tasks"
import { manifest } from "../src/worker/manifest"

test("every task has an implementation", () => {
  expect(findUnimplementedTasks(tasks, manifest)).toEqual([])
})
```

It takes the tasks among the exports of the module, and one manifest or an array of the manifests of all your workers.
