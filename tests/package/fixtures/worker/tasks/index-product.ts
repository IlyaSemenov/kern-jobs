// Implementation of a task of the consumer as the default export of its module, as the documentation lays it out, with an inferred type that its declaration names through the exports of kern-jobs.
import { RetryError, TerminalError } from "kern-jobs/worker"

import { indexProductTask } from "../../tasks.ts"

export default indexProductTask.implement(async ({ productId }, { signal, job }) => {
  if (productId === "") throw new TerminalError(`job ${job.id} has no product`)
  signal.throwIfAborted()
  if (job.attempt < job.maxAttempts) throw new RetryError("the index is busy", { delay: 10 })
})
