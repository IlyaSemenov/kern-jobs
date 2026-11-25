// Entry of the tests of an application, `kern-jobs/testing`: a test double of the jobs API and runs of a handler and of `onFailure` without a database or a worker.

export type { RunHandler, RunHandlerContext } from "./task/run-handler"
export {
  runFailure,
  type RunFailureArgs,
  type RunFailureJob,
  type RunFailureOptions,
} from "./testing/run-failure"
export { type RunJobArgs, type RunJobInfo, runJob, type RunJobOptions } from "./testing/run-job"
export {
  createTestJobs,
  type TestEnqueueOptions,
  type TestJobCall,
  type TestJobQueries,
  type TestJobs,
  type TestJobsOptions,
} from "./testing/test-jobs"
