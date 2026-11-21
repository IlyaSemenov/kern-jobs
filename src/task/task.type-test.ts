import { describe, expectTypeOf, test } from "bun:test"

import { exponentialBackoff } from "./backoff"
import type { JobFailure } from "./failure"
import {
  defineTask,
  type FailureImplementation,
  type FailureOptions,
  type HandlerContext,
  type ImplementOptions,
  type JobInfo,
  type Task,
  type TaskImplementation,
} from "./task"

describe("defineTask", () => {
  test("keeps the literal name with a payload type", () => {
    const task = defineTask("email.send").withPayload<{ to: string }>()
    expectTypeOf(task.name).toEqualTypeOf<"email.send">()
    expectTypeOf(task).toExtend<Task<"email.send", { to: string }>>()
  })

  test("defines a task without a payload", () => {
    expectTypeOf(defineTask("sessions.purgeExpired")).toExtend<
      Task<"sessions.purgeExpired", void>
    >()
  })

  test("takes the settings and the options of each enqueue, which the types of the options check", () => {
    const task = defineTask("t", { lease: 60, backoff: exponentialBackoff(), delay: 5, key: "k" })
    expectTypeOf(task.name).toEqualTypeOf<"t">()
    // @ts-expect-error debounce needs maxWait.
    defineTask("t", { key: "k", keyMode: "debounce", delay: 2 })
    // @ts-expect-error The backoff is a function of the attempt.
    defineTask("t", { backoff: 5 })
  })

  test("gives withOptions the payload", () => {
    defineTask("search.indexProduct")
      .withPayload<{ productId: string }>()
      .withOptions(payload => {
        expectTypeOf(payload).toEqualTypeOf<{ productId: string }>()
        return { key: `product:${payload.productId}` }
      })
  })

  test("takes only a function in withOptions, which returns no settings", () => {
    const task = defineTask("t").withPayload<{ seconds: number }>()
    // @ts-expect-error Static options belong to defineTask.
    task.withOptions({ delay: 60 })
    // @ts-expect-error The lease belongs to defineTask, since a worker needs it before it reads a payload.
    task.withOptions(p => ({ lease: p.seconds, key: "k" }))
    // @ts-expect-error The backoff belongs to defineTask.
    task.withOptions(() => ({ backoff: () => 1, delay: 0 }))
    // @ts-expect-error Whether the task is scoped belongs to defineTask.
    task.withOptions(() => ({ scoped: true }))
  })

  test("takes the payload type before the options only", () => {
    const task = defineTask("t").withOptions(() => ({}))
    // @ts-expect-error The options were given for the payload type of the task.
    task.withPayload<{ id: string }>()
  })

  test("gives a task of any payload as a Task<string, unknown>", () => {
    const task = defineTask("t")
      .withPayload<{ id: string }>()
      .withOptions(({ id }) => ({ key: id }))
    expectTypeOf(task).toExtend<Task<string, unknown>>()
  })
})

describe("implement", () => {
  const task = defineTask("search.indexProduct").withPayload<{ productId: string }>()

  test("gives the handler the payload and the context of its job", () => {
    const implementation = task.implement((payload, context) => {
      expectTypeOf(payload).toEqualTypeOf<{ productId: string }>()
      expectTypeOf(context).toEqualTypeOf<HandlerContext>()
      expectTypeOf(context.signal).toEqualTypeOf<AbortSignal>()
      expectTypeOf(context.job).toEqualTypeOf<JobInfo>()
    })
    expectTypeOf(implementation).toEqualTypeOf<
      TaskImplementation<"search.indexProduct", { productId: string }>
    >()
  })

  test("gives the handler of a scoped task the scope as a string, through withPayload and withOptions", () => {
    defineTask("search.indexProduct", { scoped: true })
      .withPayload<{ productId: string }>()
      .withOptions(({ productId }) => ({ key: productId }))
      .implement((_, { job }) => {
        expectTypeOf(job.scope).toEqualTypeOf<string>()
      })
    defineTask("t", { scoped: false }).implement((_, { job }) => {
      expectTypeOf(job.scope).toEqualTypeOf<string | null>()
    })
  })

  test("takes a handler that returns a value or a promise of one", () => {
    task.implement(async () => 42)
    task.implement(() => "done")
  })

  test("rejects a handler of another payload", () => {
    // @ts-expect-error The handler takes the payload of the task.
    task.implement((payload: { id: number }) => payload)
  })

  test("gives an implementation of any payload as a TaskImplementation<string, unknown>", () => {
    expectTypeOf(task.implement(() => {})).toExtend<TaskImplementation<string, unknown>>()
    expectTypeOf(defineTask("t").implement(() => {})).toExtend<
      TaskImplementation<string, unknown>
    >()
    expectTypeOf(defineTask("t", { scoped: true }).implement(() => {})).toExtend<
      TaskImplementation<string, unknown>
    >()
  })
})

describe("implement with onFailure", () => {
  const charge = defineTask("orders.charge").withPayload<{ orderId: string }>()

  test("gives onFailure the failure of a job of the task, with its payload, and the context of the failure job", () => {
    const implementation = charge.implement(() => {}, {
      onFailure: (failure, context) => {
        expectTypeOf(failure).toEqualTypeOf<JobFailure<{ orderId: string }>>()
        expectTypeOf(failure.payload).toEqualTypeOf<{ orderId: string }>()
        expectTypeOf(failure.job.scope).toEqualTypeOf<string | null>()
        expectTypeOf(context).toEqualTypeOf<HandlerContext>()
      },
    })
    expectTypeOf(implementation.failure).toEqualTypeOf<
      FailureImplementation<"orders.charge", { orderId: string }>
    >()
    expectTypeOf(implementation.failure.task.name).toEqualTypeOf<"orders.charge:failure">()
    expectTypeOf(implementation).toExtend<
      TaskImplementation<"orders.charge", { orderId: string }>
    >()
  })

  test("gives the handler of an onFailure given as an object the same failure and context, and the same implementation", () => {
    const implementation = charge.implement(() => {}, {
      onFailure: {
        maxAttempts: 100,
        backoff: exponentialBackoff({ max: 60 }),
        handler: (failure, context) => {
          expectTypeOf(failure).toEqualTypeOf<JobFailure<{ orderId: string }>>()
          expectTypeOf(context).toEqualTypeOf<HandlerContext>()
        },
      },
    })
    expectTypeOf(implementation.failure).toEqualTypeOf<
      FailureImplementation<"orders.charge", { orderId: string }>
    >()
    const options: FailureOptions<{ orderId: string }> = { handler: () => {}, maxAttempts: 3 }
    expectTypeOf(charge.implement(() => {}, { onFailure: options }).failure).toEqualTypeOf<
      FailureImplementation<"orders.charge", { orderId: string }>
    >()
    // @ts-expect-error The lease of failure jobs is not a setting.
    charge.implement(() => {}, { onFailure: { handler: () => {}, lease: 60 } })
    // @ts-expect-error An object needs the handler.
    charge.implement(() => {}, { onFailure: { maxAttempts: 3 } })
  })

  test("gives the failure of a job of a task without a payload no payload", () => {
    defineTask("sessions.purge").implement(() => {}, {
      onFailure: failure => {
        expectTypeOf(failure.payload).toEqualTypeOf<void>()
      },
    })
  })

  test("gives onFailure of a scoped task the scope of the failure job as a string", () => {
    const scopedCharge = defineTask("orders.charge", { scoped: true }).withPayload<{
      orderId: string
    }>()
    const implementation = scopedCharge.implement(() => {}, {
      onFailure: (failure, { job }) => {
        expectTypeOf(job.scope).toEqualTypeOf<string>()
        // A job may predate the scope of its task.
        expectTypeOf(failure.job.scope).toEqualTypeOf<string | null>()
      },
    })
    expectTypeOf(implementation.failure).toEqualTypeOf<
      FailureImplementation<"orders.charge", { orderId: string }, string>
    >()
  })

  test("rejects an onFailure that is not a handler of the failures of the task", () => {
    const failed = defineTask("orders.charge.failed").withPayload<JobFailure<{ orderId: string }>>()
    // @ts-expect-error A task is not a handler.
    charge.implement(() => {}, { onFailure: failed })
    // @ts-expect-error A handler of the failures of jobs of another payload.
    charge.implement(() => {}, { onFailure: (failure: JobFailure<{ id: number }>) => failure })
  })

  test("gives an implementation whose options may lack onFailure a failure that may be absent", () => {
    const options: ImplementOptions<{ orderId: string }> = { onFailure: () => {} }
    const implementation = charge.implement(() => {}, options)
    expectTypeOf(implementation).toEqualTypeOf<
      TaskImplementation<"orders.charge", { orderId: string }>
    >()
    expectTypeOf(implementation.failure).toEqualTypeOf<
      FailureImplementation<"orders.charge", { orderId: string }> | undefined
    >()
    expectTypeOf(charge.implement(() => {})).toEqualTypeOf<
      TaskImplementation<"orders.charge", { orderId: string }>
    >()
  })

  test("takes an implementation with a failure task, and its failure implementation, as an implementation of any task", () => {
    const implementation = charge.implement(() => {}, { onFailure: () => {} })
    const any: TaskImplementation<string, unknown>[] = [implementation, implementation.failure]
    expectTypeOf(any).toBeArray()
  })

  test("keeps the failure task of an implementation that a wrapper spreads", () => {
    const implementation = charge.implement(() => {}, { onFailure: () => {} })
    const wrapped = {
      ...implementation,
      handler: (payload: { orderId: string }, context: HandlerContext) =>
        implementation.handler(payload, context),
    }
    expectTypeOf(wrapped.failure).toEqualTypeOf<
      FailureImplementation<"orders.charge", { orderId: string }>
    >()
  })
})
