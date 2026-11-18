# Scheduling Recurring Jobs

Register a schedule in the worker manifest to enqueue a task on a UTC calendar.

```ts
import { defineTask } from "kern-jobs"
import { defineManifest, defineSchedule } from "kern-jobs/worker"

const backupTask = defineTask("backup").withPayload<{ destination: string }>()
const backup = backupTask.implement(async ({ destination }) => {
  await createBackup(destination)
})

export const manifest = defineManifest({
  tasks: [backup],
  schedules: [
    defineSchedule(backupTask, {
      id: "weekly-backup",
      cron: { minute: 0, hour: 4, dayOfWeek: "sun" },
      payload: { destination: "archive" },
      enqueue: { queue: "backups", maxAttempts: 1 },
    }),
  ],
})
```

Give each schedule of the same task its own `id`: it defaults to the task name and identifies the schedule across your workers.
The task can run on any worker of the same schema, so a worker that schedules it need not implement it; [check in a test](./#check-the-manifest-in-tests) that some manifest does.

A schedule enqueues an unscoped task.
To dispatch work per tenant, have that task enumerate tenants and enqueue a [scoped task](../tasks/scopes) for each one.

## Define a UTC calendar

`cron` takes a structured calendar or a five-field cron string, both in UTC.
A structured calendar requires `minute`; pass `"*"` for every minute.
An omitted field selects all its values.

- `{ minute: 15 }` runs at :15 every hour.
- `{ minute: { every: 10 }, hour: { from: 9, to: 17 }, dayOfWeek: { from: "mon", to: "fri" } }` runs every ten minutes from 09:00 to 17:50, Monday to Friday.
- `{ minute: [0, 30], hour: 8, month: [3, 9], dayOfMonth: 1 }` runs at 08:00 and 08:30 on March 1 and September 1.

Weekdays run from 0 for Sunday to 6, with 7 also meaning Sunday, or from `sun` to `sat`.
A string such as `"0 4 * * SUN"` takes the usual cron syntax, including lists, ranges, and steps such as `*/10` or `9-17/2`.

When you restrict both `dayOfMonth` and `dayOfWeek`, either one selects a day: `{ minute: 0, hour: 0, dayOfMonth: 13, dayOfWeek: "fri" }` runs on every Friday and on every 13th, not only on Friday the 13th.
A field that lists all its values, such as `0-6` for weekdays, counts as `*`, so `"0 0 13 * 0-6"` runs only on the 13th.

## Set job options

Use `enqueue` on the schedule to [override the task options](../tasks/#overriding-options).

A schedule can enqueue a job while its previous job is pending or running.
Give it a [key](../tasks/keys) to merge it into a pending job, or a [queue](../tasks/queues) to run one job at a time.

If the task uses `withOptions`, the worker calls it for each scheduled enqueue, possibly more than once, and reports any error it throws as a [`schedule` error](./monitoring#report-errors).
Keep that function fast and free of side effects.

## Recover missed occurrences

Workers check their schedules about every 15 seconds, also while their handlers are busy.
Several workers may check the same minute, but only one check commits the decision whether to enqueue a job for it, so the schedule enqueues at most one job per minute, also after its previous job completes.

If no check of the schedule commits during the minute of an occurrence, such as during downtime or after a database error, the occurrence is skipped.
Pass `catchUp: { within: 3600 }` to enqueue it if a worker checks the schedule within the next hour.
A check enqueues at most one job however many occurrences it recovers, so write the task to process everything that is due rather than a single occurrence.
A new schedule starts at the minute of its first committed check and recovers nothing before it, also when an earlier check failed.

## Pause a schedule

Keep the schedule in the manifest with `enabled: false` to pause it: the minutes that workers check while it is paused stay skipped after you resume it, also with `catchUp`.
A schedule removed from every manifest is not paused: once it returns, `catchUp` recovers its missed occurrences as after downtime.

## Change a schedule

Give a schedule the same definition on all workers that list its `id`, including `enabled`, the payload, and the options.
During a rolling deployment, whichever worker checks a minute first decides whether to enqueue a job for it, so an occurrence of only the new definition can be skipped, and one of only the old definition can still run.
Under the same `id` with `catchUp`, a changed definition also applies to the occurrences missed before the change.

To change a schedule without this race, do one of the following:

- Stop every worker that lists its `id`, then start the workers with the new definition.
  With `catchUp`, the new definition recovers the occurrences of its own calendar that the stop missed within the window.
- Give the changed schedule a new `id`.
  Until the old workers stop, both schedules enqueue, so a minute that matches both gets two jobs.
  The new `id` starts at the minute of its first committed check, without recovering what the old one missed.
