// Migrations of kern-jobs: every migration applies, rolls back to the structure it found, and applies again.
import { test } from "bun:test"

import { verifyMigrations } from "kern-migrations/testing"

import { createTestSchema } from "../core/testing"

import { jobMigrations } from "./migrations"

test("every migration applies, rolls back to the structure before it, and applies again", async () => {
  const schema = await createTestSchema()
  try {
    await verifyMigrations(schema.database, jobMigrations)
  } finally {
    await schema.dispose()
  }
})
