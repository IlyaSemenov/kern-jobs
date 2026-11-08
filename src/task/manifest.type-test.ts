import { describe, expectTypeOf, test } from "bun:test"

import { defineManifest, type Manifest } from "./manifest"
import { defineTask } from "./task"

const indexProduct = defineTask("search.indexProduct").withPayload<{ productId: string }>()
const purgeExpiredSessions = defineTask("sessions.purgeExpired")

describe("defineManifest", () => {
  test("takes implementations of tasks of any payloads", () => {
    const manifest = defineManifest({
      tasks: [indexProduct.implement(() => {}), purgeExpiredSessions.implement(() => {})],
    })
    expectTypeOf(manifest).toEqualTypeOf<Manifest>()
  })

  test("rejects a task without its implementation", () => {
    // @ts-expect-error A manifest takes implementations, not definitions.
    defineManifest({ tasks: [indexProduct] })
  })
})
