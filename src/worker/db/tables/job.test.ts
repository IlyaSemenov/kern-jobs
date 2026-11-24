// Job table of the worker ORM schema: the times of its rows.
import { afterEach, beforeEach, expect, test } from "bun:test"

import { createTestSchema, type TestSchema } from "../../../test-harness/database"
import { query } from "../query"

let schema: TestSchema
beforeEach(async () => {
  schema = await createTestSchema()
})
afterEach(() => schema.dispose())

test("reads the fractional seconds of the times of a job on a pool whose DateStyle and TimeZone are not the defaults", async () => {
  // In Asia/Kathmandu, these instants of January 2 fall on January 3, which SQL, DMY writes day first.
  const db = schema.createDb({ dateStyle: "SQL, DMY", timeZone: "Asia/Kathmandu" })
  await query(
    schema.admin,
    `insert into ${schema.table.name} (task, run_at, window_started_at, created_at, locked_at, max_attempts)
     values ('t', '2030-01-02 19:58:00.1236+00', '2030-01-02 19:57:00.1234+00', '2030-01-02 19:56:00+00', null, 1)`,
  )
  expect(await db.job.select("runAt", "windowStartedAt", "createdAt", "lockedAt").take()).toEqual({
    runAt: new Date("2030-01-02T19:58:00.124Z"),
    windowStartedAt: new Date("2030-01-02T19:57:00.123Z"),
    createdAt: new Date("2030-01-02T19:56:00Z"),
    lockedAt: null,
  })
})
