import type { BatchEnqueueOverrides, EnqueueOverrides } from "../task/options"
import { defineTask } from "../task/task"

import { defineSchedule, type ScheduleOptions } from "./schedule"
const task = defineTask("region").withPayload<{ region: string }>()
defineSchedule(task, { cron: { minute: 0 }, payload: { region: "eu" } })
// @ts-expect-error The task requires its payload.
defineSchedule(task, { cron: { minute: 0 } })
// @ts-expect-error The options cannot widen the payload type inferred from the task.
defineSchedule(task, { cron: { minute: 0 }, payload: { region: 5 } })
// @ts-expect-error The lease belongs to the task.
defineSchedule(task, { cron: { minute: 0 }, payload: { region: "eu" }, enqueue: { lease: 5 } })
// A call that gives the payload type alone compiles.
defineSchedule<{ region: string }>(task, { cron: { minute: 0 }, payload: { region: "eu" } })
// Options typed without a scope type are those of a task that is not scoped.
const taskOptions: ScheduleOptions<{ region: string }> = {
  cron: { minute: 0 },
  payload: { region: "eu" },
}
defineSchedule(task, taskOptions)

const digest = defineTask("digest", { key: true }).withBatchPayload<string>({ maxItems: 10 })
defineSchedule(digest, { cron: { minute: 0 }, payload: ["tick"], enqueue: { key: "k" } })
// @ts-expect-error The other options apply to the whole batch and come from defineTask.
defineSchedule(digest, { cron: { minute: 0 }, payload: ["tick"], enqueue: { delay: 5 } })
// @ts-expect-error The payload of a batch task is its array of items.
defineSchedule(digest, { cron: { minute: 0 }, payload: "tick" })
// @ts-expect-error The items of a batch task append to the pending job of their key.
defineSchedule(digest, { cron: { minute: 0 }, payload: ["tick"], enqueue: { key: null } })
// A call that gives the item type alone compiles.
defineSchedule<string>(digest, { cron: { minute: 0 }, payload: ["tick"] })
// Options typed without a scope type describe no scopes, as those of a batch task do.
const digestOptions: ScheduleOptions<string[], BatchEnqueueOverrides> = {
  cron: { minute: 0 },
  payload: ["tick"],
}
defineSchedule(digest, digestOptions)
// @ts-expect-error A schedule of a batch task takes no scopes.
defineSchedule(digest, { cron: { minute: 0 }, payload: ["tick"], scopes: ["a"] })
const lists = defineTask("lists").withPayload<string[]>()
defineSchedule(lists, { cron: { minute: 0 }, payload: ["a"], enqueue: { key: null } })

const scoped = defineTask("metrics.recalculate", { scoped: true }).withPayload<{ day: string }>()
defineSchedule(scoped, { cron: { minute: 0 }, payload: { day: "today" }, scopes: ["a"] })
defineSchedule(scoped, { cron: { minute: 0 }, payload: { day: "today" }, scopes: () => ["a"] })
defineSchedule(scoped, {
  cron: { minute: 0 },
  payload: { day: "today" },
  scopes: async (): Promise<readonly string[]> => ["a"],
})
// @ts-expect-error A scoped task requires the scopes of its jobs.
defineSchedule(scoped, { cron: { minute: 0 }, payload: { day: "today" } })
// Options typed with the scope type of a scoped task require the scopes too.
const scopedOptions: ScheduleOptions<{ day: string }, EnqueueOverrides, string> = {
  cron: { minute: 0 },
  payload: { day: "today" },
  scopes: ["a"],
}
defineSchedule(scoped, scopedOptions)
const unscopedOptions: ScheduleOptions<{ day: string }> = {
  cron: { minute: 0 },
  payload: { day: "today" },
}
// @ts-expect-error Options typed without a scope type describe no scopes, which a scoped task requires.
defineSchedule(scoped, unscopedOptions)
// @ts-expect-error A scope is a string.
defineSchedule(scoped, { cron: { minute: 0 }, payload: { day: "today" }, scopes: [5] })
// @ts-expect-error The function of the scopes takes no arguments.
defineSchedule(scoped, {
  cron: { minute: 0 },
  payload: { day: "today" },
  scopes: (id: string) => [id],
})
// A call that gives the payload type alone leaves the check of the scopes to the definition.
defineSchedule<{ day: string }>(scoped, { cron: { minute: 0 }, payload: { day: "today" } })
// A task whose type does not tell whether it is scoped takes the scopes that it then requires.
const flagged = defineTask("flagged", { scoped: Math.random() < 2 })
defineSchedule(flagged, { cron: { minute: 0 }, scopes: Math.random() < 2 ? ["a"] : undefined })
