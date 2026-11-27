# A schedule gives the scopes of a scoped task

Give a schedule of a scoped task its scopes, an array or a function that returns one, and enqueue the task in each of them in the transaction that advances the cursor.
The job of each scope is an ordinary job of the task: it has its own attempts, backoff, and failure, and its handler, or `runHandler` of the worker, reads its scope as `job.scope`.
One cursor serves every scope, so a scope added later has no occurrences of its own to recover, and an enqueue that fails in one scope rolls back the others.

## A dispatcher task

The schedule could enqueue only tasks that are not scoped, and the application a task that lists its tenants and enqueues a scoped task for each.
It needs nothing of the schedule, and the list is read in a handler, with its retries.
Every application with tenants writes that task, its enqueues, and the handling of a failure after some of the enqueues, and the list of tenants is read by a job that may run long after its occurrence.
Graphile Worker, Inngest, and Sidekiq document such a task ([comparison](../../comparison/schedules.md#a-recurring-job-for-each-tenant)).

## A source of scopes of the worker

`startWorker` could take one source of scopes for every schedule of scoped tasks.
It saves an option on each schedule.
The scopes are then part of the configuration of each worker rather than of the definition that the workers share by its ID, a schedule cannot enqueue for only some of the scopes, and a schedule without a source is found only when a worker starts.

## A cursor per scope

A row per schedule and scope would let each scope commit, fail, and catch up on its own, and bound each transaction to one scope.
The rows grow with the scopes, a check takes a transaction per scope each round, and a scope first seen in the middle of a catch-up window needs a rule of its own.

## A key of the occurrence and the scope

The job of each scope could take a key of the schedule, the minute, and the scope, so that a repeated fan-out merges instead of the cursor preventing it.
Completing a job deletes it, so a repeated check of the minute would enqueue it again, as for [a key in place of the cursor](occurrence-cursor.md), and the key would replace that of the task.
