// Manifest of the worker of the consumer and the check of its tasks, exported with inferred types that its declarations name through the exports of kern-jobs; the type tests of `declaration-consumer/` check the emitted types.
import { defineManifest, defineSchedule, findUnimplementedTasks } from "kern-jobs/worker"

import * as tasks from "../tasks.ts"

import indexProduct from "./tasks/index-product.ts"
import purgeExpiredSessions from "./tasks/purge-expired-sessions.ts"

/** Schedule exported through the declaration of a worker module. */
export const weeklyCleanup = defineSchedule(purgeExpiredSessions.task, {
  cron: { minute: 0, hour: 0, dayOfWeek: "sun" },
})

/** Schedule of a wrapper that passes its optional parameters through, also as `undefined`. */
export function createCleanupSchedule(id?: string, from?: number, catchUp?: { within: number }) {
  return defineSchedule(purgeExpiredSessions.task, {
    id,
    cron: { minute: { every: 15, from } },
    catchUp,
  })
}

export const manifest = defineManifest({ tasks: [indexProduct, purgeExpiredSessions] })

/** Names of the tasks of the consumer that its manifest does not implement. */
export const unimplementedTasks = findUnimplementedTasks(tasks, manifest)
