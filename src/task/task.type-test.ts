import { describe, expectTypeOf, test } from "bun:test"

import { exponentialBackoff } from "./backoff"
import {
  defineTask,
  type HandlerContext,
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
