// Delay of a job: the seconds that a statement adds to its time for the run time of the job, such as of an enqueue or of a retry.

/** Longest delay in seconds, about 31,700 years, a bound well within the intervals and timestamps of PostgreSQL. */
const maxDelay = 1e12

/** Range of a delay, for error messages. */
export const delayRange = "a number of seconds from 0 to 1e12"

/** Whether a value is a delay: a number of seconds from 0 to `maxDelay`. */
export function isDelay(value: unknown): value is number {
  return typeof value === "number" && value >= 0 && value <= maxDelay
}
