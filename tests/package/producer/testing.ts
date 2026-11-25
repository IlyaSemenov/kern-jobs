// Records an enqueue and runs a handler through `kern-jobs/testing` in a project that has neither the dependencies of kern-jobs nor a client library, and prints the outcome as JSON.

import { defineTask } from "kern-jobs"
import { createTestJobs, runJob } from "kern-jobs/testing"

const indexProductTask = defineTask("search.indexProduct", { scoped: true })
  .withPayload<{ productId: string }>()
  .withOptions(({ productId }) => ({ key: `product:${productId}` }))
const testJobs = createTestJobs({ scope: "tenant" })
const captured = await testJobs.capture(() =>
  testJobs.enqueue(indexProductTask, { productId: "1" }),
)
const job = await runJob(
  indexProductTask.implement((_payload, { job }) => job),
  { productId: "1" },
  { job: { scope: "tenant" } },
)
console.log(JSON.stringify({ calls: captured.callsFor(indexProductTask), job }))
