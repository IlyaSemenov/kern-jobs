// Run of the handler of a job through the `runHandler` of a worker, which the worker and `runJob` of `kern-jobs/testing` share.

import type { HandlerContext, TaskImplementation } from "./task"

/** What `runHandler` of a worker gets about a job whose handler it runs: the context of the handler, the name of the task, and the payload of the job. */
export interface RunHandlerContext extends HandlerContext {
  task: string
  payload: unknown
}

/**
 * Wraps the handler of every job of a worker, such as to run it in the context of the scope of the job: `run` runs the handler and returns its promise, which rejects also when the handler throws synchronously.
 * The worker settles the job by the outcome of `runHandler` as by the outcome of a handler, also after the abort of its signal, so call `run` at most once and return or await its promise.
 * A `runHandler` that resolves without calling `run` completes the job without running its handler.
 */
export type RunHandler = (context: RunHandlerContext, run: () => Promise<unknown>) => unknown

/**
 * Runs the handler of an implementation with the payload and the metadata of a context, through `runHandler` when it is given.
 * Without `runHandler`, it returns the promise of `run`, which rejects also when the handler throws synchronously; with `runHandler`, it returns what `runHandler` returns.
 * The handler gets the signal and the metadata of the context without the task and the payload.
 */
export function runTaskHandler(
  implementation: Pick<TaskImplementation<string, unknown>, "handler">,
  context: RunHandlerContext,
  runHandler: RunHandler | undefined,
): unknown {
  const { signal, job, payload } = context
  const handlerContext: HandlerContext = { signal, job }
  const run = async () => await implementation.handler(payload, handlerContext)
  return runHandler ? runHandler(context, run) : run()
}
