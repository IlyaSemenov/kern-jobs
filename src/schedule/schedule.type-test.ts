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
