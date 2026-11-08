// Entry of the worker process, `kern-jobs/worker`: what only a worker runs, apart from the definitions and the enqueue of the main entry, which producers import.
// The type of the driver that `startWorker` takes, so that an application names it without depending on kern-db.
export type { Driver } from "kern-db"
export { RetryError, type RetryErrorOptions, TerminalError } from "./task/errors"
export { defineManifest, findUnimplementedTasks, type Manifest } from "./task/manifest"
export type { AbortReason } from "./worker/events"
export { HandlerAbort } from "./worker/heartbeat"
export type { StopOptions, Worker, WorkerState } from "./worker/runtime"
export { stopOnSignals, type StopOnSignalsOptions } from "./worker/shutdown"
export {
  startWorker,
  type WorkerErrorInfo,
  type WorkerOptions,
  type WorkerReaperOptions,
} from "./worker/worker"
