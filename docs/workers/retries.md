# Retries

A job whose handler throws runs again after the backoff of its task: 2 seconds after the first attempt, doubled after each next one, up to an hour.
After its last attempt, [`maxAttempts`](#attempts) of its task, it stays in the database as failed with the message of the error, until you [retry or delete it](./failed-jobs).

## Attempts

To limit how many times a failing job runs, pass `maxAttempts`, 25 by default:

```ts
export const chargeCardTask = defineTask("payments.charge", { maxAttempts: 3 }).withPayload<{
  paymentId: string
}>()
```

An enqueue with `replace` or `debounce` keeps the attempts that its pending job has spent, also while the job waits for a retry; pass `resetAttempts: true` to give it a full budget again.
Attempts belong to one job: a failed job that a newer event [supersedes](../tasks/keys#while-a-job-of-the-key-runs) leaves its attempts behind, and the pending job runs on its own, so a key whose jobs keep failing retries for as long as events keep arriving.
A job whose `maxAttempts` an enqueue lowers below the attempts it spent gets one more attempt.

## Backoff

To retry on another schedule, pass `backoff` to `defineTask`:

```ts
import { defineTask, exponentialBackoff, fixedBackoff } from "kern-jobs"

export const renderPreviewTask = defineTask("documents.renderPreview", {
  backoff: exponentialBackoff({ base: 10, max: 600, jitter: 0.2 }),
}).withPayload<{ documentId: string }>()

export const purgeExpiredSessionsTask = defineTask("sessions.purgeExpired", {
  backoff: fixedBackoff(60),
})
```

A backoff is a function of the number of the failed attempt that returns seconds, so you can also pass your own.

## Decide in the handler

To fail a job for good or retry it on your own schedule, throw one of these errors in the handler:

- `TerminalError` fails the job for good, such as for a payload that can never be processed.
- `RetryError` retries the job after the delay that you give it, such as the one that a service asks for.

```ts [src/worker/tasks/sync-account.ts]
import { RetryError, TerminalError } from "kern-jobs/worker"

import { syncAccountTask } from "../../tasks"

export default syncAccountTask.implement(async ({ accountId }, { signal }) => {
  const response = await fetch(`https://api.example.com/accounts/${accountId}`, { signal })
  if (response.status === 404) throw new TerminalError(`account ${accountId} does not exist`)
  if (response.status === 429) {
    // This service sends `Retry-After` in seconds.
    const delay = Number(response.headers.get("Retry-After") ?? 60)
    throw new RetryError("the service limits requests", { delay })
  }
  if (!response.ok) throw new Error(`the service responded with ${response.status}`)
  await storeAccount(accountId, await response.json())
})
```

A retry after `RetryError` spends an attempt like any other failure.
