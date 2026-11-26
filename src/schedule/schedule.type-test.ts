import { defineTask } from "../task/task"

import { defineSchedule } from "./schedule"
const task = defineTask("region").withPayload<{ region: string }>()
defineSchedule(task, { cron: { minute: 0 }, payload: { region: "eu" } })
// @ts-expect-error The task requires its payload.
defineSchedule(task, { cron: { minute: 0 } })
// @ts-expect-error The options cannot widen the payload type inferred from the task.
defineSchedule(task, { cron: { minute: 0 }, payload: { region: 5 } })
// @ts-expect-error The lease belongs to the task.
defineSchedule(task, { cron: { minute: 0 }, payload: { region: "eu" }, enqueue: { lease: 5 } })

const digest = defineTask("digest", { key: true }).withBatchPayload<string>({ maxItems: 10 })
defineSchedule(digest, { cron: { minute: 0 }, payload: ["tick"], enqueue: { key: "k" } })
// @ts-expect-error The other options apply to the whole batch and come from defineTask.
defineSchedule(digest, { cron: { minute: 0 }, payload: ["tick"], enqueue: { delay: 5 } })
// @ts-expect-error The payload of a batch task is its array of items.
defineSchedule(digest, { cron: { minute: 0 }, payload: "tick" })
// @ts-expect-error The items of a batch task append to the pending job of their key.
defineSchedule(digest, { cron: { minute: 0 }, payload: ["tick"], enqueue: { key: null } })
const lists = defineTask("lists").withPayload<string[]>()
defineSchedule(lists, { cron: { minute: 0 }, payload: ["a"], enqueue: { key: null } })
