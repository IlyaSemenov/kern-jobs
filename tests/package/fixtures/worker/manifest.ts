// Manifest of the worker of the consumer and the check of its tasks, exported with inferred types that its declarations name through the exports of kern-jobs; the type tests of `declaration-consumer/` check the emitted types.
import { defineManifest, findUnimplementedTasks } from "kern-jobs/worker"

import * as tasks from "../tasks.ts"

import indexProduct from "./tasks/index-product.ts"
import purgeExpiredSessions from "./tasks/purge-expired-sessions.ts"

export const manifest = defineManifest({ tasks: [indexProduct, purgeExpiredSessions] })

/** Names of the tasks of the consumer that its manifest does not implement. */
export const unimplementedTasks = findUnimplementedTasks(tasks, manifest)
