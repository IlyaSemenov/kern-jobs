// Task definitions of the consumer, exported with inferred types that its declarations name through the exports of kern-jobs; the type tests of `declaration-consumer/` check the emitted types.
import { defineTask, exponentialBackoff, fixedBackoff } from "kern-jobs"

export const purgeExpiredSessionsTask = defineTask("sessions.purgeExpired", {
  lease: 300,
  backoff: fixedBackoff(60),
})

export const indexBackoff = exponentialBackoff({ base: 10, jitter: 0.2 })

export const indexProductTask = defineTask("search.indexProduct", {
  delay: 5,
  backoff: indexBackoff,
})
  .withPayload<{ productId: string }>()
  .withOptions(({ productId }) => ({ key: `product:${productId}` }))

// Under `exactOptionalPropertyTypes`, the options must admit a field given as `undefined`.
export const sendReminderTask = defineTask("reminders.send")
  .withPayload<{ reminderId: string; at?: string }>()
  .withOptions(({ at }) => ({ runAt: at === undefined ? undefined : new Date(at) }))

/** Defines a cleanup task, passing its optional lease through as it is. */
export function defineCleanup<const Name extends string>(name: Name, lease?: number) {
  return defineTask(name, { lease, delay: 60 })
}
