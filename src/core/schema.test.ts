// Names of the PostgreSQL schema of the job table: their check, and the statements of the core in a schema whose name needs quoting.
import { expect, test } from "bun:test"

import { migrate } from "kern-migrations"

import { jobMigrations } from "../migration/migrations"

import { claimPlain } from "./claim"
import { enqueue } from "./enqueue"
import { checkSchemaName } from "./schema"
import { complete } from "./settlement"
import { createTestSchema, only } from "./testing"

test("the statements of the core run in a schema whose name needs quoting", async () => {
  const schema = await createTestSchema()
  const table = schema.unmigratedTable(`Kern "jobs" 'q'`)
  try {
    await migrate(schema.database, jobMigrations, { schemaName: table.schemaName })
    const { id } = await enqueue(schema.admin, table, { task: "t" })
    const claim = only(
      await claimPlain(schema.admin, table, { worker: "w", tasks: ["t"], limit: 1 }),
    )
    expect(claim.id).toBe(id)
    expect(await complete(schema.admin, table, claim)).toBe(true)
  } finally {
    await schema.dispose()
  }
})

test("checkSchemaName counts the length of a name in bytes of UTF-8, up to 63, and rejects U+0000", () => {
  // "é" takes 2 bytes, and "😀" takes 4.
  for (const name of ["a", "a".repeat(63), "é".repeat(31) + "a", "😀".repeat(15) + "abc"]) {
    expect(() => checkSchemaName(name)).not.toThrow()
  }
  for (const name of ["", "a".repeat(64), "é".repeat(32), "😀".repeat(16), "a\0b", null, 5]) {
    expect(() => checkSchemaName(name)).toThrow(
      new TypeError("schemaName must be a string of 1 to 63 bytes in UTF-8 without U+0000"),
    )
  }
})

test("a name of 63 bytes in UTF-8 names its schema without truncation", async () => {
  const schema = await createTestSchema()
  // `unmigratedTable` prefixes the label with the name of the test schema and "_", which are ASCII.
  const free = 63 - schema.name.length - 1
  const table = schema.unmigratedTable("é".repeat(Math.floor(free / 2)) + "a".repeat(free % 2))
  try {
    expect(new TextEncoder().encode(table.schemaName).length).toBe(63)
    await migrate(schema.database, jobMigrations, { schemaName: table.schemaName })
    expect(
      await schema.database.query`select from pg_namespace where nspname = ${table.schemaName}`,
    ).toBe(1)
  } finally {
    await schema.dispose()
  }
})
