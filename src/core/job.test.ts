// Check constraints of the job table, which reject a row that breaks an invariant of the data model whatever statement writes it.
import { afterEach, beforeEach, describe, expect, test } from "bun:test"

import { isQueryError } from "kern-db"

import { claimQueued } from "./claim"
import { enqueue, type EnqueueParams } from "./enqueue"
import { fail } from "./settlement"
import { query } from "./statement"
import { createRandom, createTestSchema, jobs, only, T, type TestSchema } from "./testing"

let schema: TestSchema
beforeEach(async () => {
  schema = await createTestSchema()
})
afterEach(() => schema.dispose())

/** Returns the constraint that rejected a write, or `null` when the write succeeded. */
async function rejectedBy(write: Promise<unknown>): Promise<string | null> {
  try {
    await write
    return null
  } catch (error) {
    if (isQueryError(error, "check_violation")) return error.constraint ?? "unnamed"
    throw error
  }
}

/** Columns of a row that the test inserts; the others take their values from `insertRow`. */
interface RowFields {
  state?: string
  claim_id?: string | null
  lease_until?: string | null
  attempts?: number
  max_attempts?: number
  run_at?: string
  not_before?: string | null
  failed_at?: string | null
}

/**
 * Inserts a pending row without a claim, with the fields given over it, and returns the constraint that rejected it, or `null`; a failed row has a `failed_at` unless the fields give one.
 */
async function insertRow(fields: RowFields): Promise<string | null> {
  const row = {
    state: "pending",
    claim_id: null,
    lease_until: null,
    attempts: 0,
    max_attempts: 1,
    run_at: "2030-01-01T00:00:00Z",
    not_before: null,
    failed_at: fields.state === "failed" ? "2030-01-01T00:00:00Z" : null,
    ...fields,
  }
  return rejectedBy(
    query(
      schema.admin,
      `insert into ${schema.table.name}
         (task, state, claim_id, lease_until, attempts, max_attempts, run_at, not_before, failed_at, window_started_at, created_at)
       values ('t', $1::text, $2::text::bigint, $3::text::timestamptz, $4::text::int, $5::text::int,
               $6::text::timestamptz, $7::text::timestamptz, $8::text::timestamptz, now(), now())`,
      [
        row.state,
        row.claim_id,
        row.lease_until,
        row.attempts,
        row.max_attempts,
        row.run_at,
        row.not_before,
        row.failed_at,
      ],
    ),
  )
}

test("a row has claim_id and lease_until exactly while it is active", async () => {
  const claim = "1"
  const lease = "2030-01-01T00:00:00Z"
  for (const state of ["pending", "active", "failed"]) {
    for (const [claimId, leaseUntil] of [
      [null, null],
      [claim, null],
      [null, lease],
      [claim, lease],
    ] as const) {
      const active = state === "active"
      const valid = active === (claimId !== null) && active === (leaseUntil !== null)
      expect({
        state,
        claimId,
        leaseUntil,
        rejectedBy: await insertRow({
          state,
          claim_id: claimId,
          lease_until: leaseUntil,
        }),
      }).toEqual({
        state,
        claimId,
        leaseUntil,
        rejectedBy: valid ? null : "job_active_check",
      })
    }
  }
})

test("a row has failed_at exactly while it is failed", async () => {
  const failedAt = "2030-01-01T00:00:00Z"
  for (const state of ["pending", "failed"]) {
    for (const value of [null, failedAt]) {
      const valid = (state === "failed") === (value !== null)
      expect([state, value, await insertRow({ state, failed_at: value })]).toEqual([
        state,
        value,
        valid ? null : "job_failed_check",
      ])
    }
  }
})

test("attempts are at least 0, and max_attempts at least 1", async () => {
  expect(await insertRow({ attempts: 0, max_attempts: 1 })).toBeNull()
  expect(await insertRow({ attempts: -1 })).toBe("job_attempts_check")
  expect(await insertRow({ max_attempts: 0 })).toBe("job_max_attempts_check")
})

test("run_at is not below not_before, the end of the backoff", async () => {
  const runAt = "2030-01-01T00:00:00Z"
  expect(await insertRow({ run_at: runAt, not_before: null })).toBeNull()
  expect(await insertRow({ run_at: runAt, not_before: runAt })).toBeNull()
  expect(await insertRow({ run_at: runAt, not_before: "2030-01-01T00:00:01Z" })).toBe(
    "job_not_before_check",
  )
})

describe("names", () => {
  /** Enqueues a job of task `t` with the names given over it, and returns the constraint that rejected it, or `null`. */
  const enqueueNames = (names: Partial<EnqueueParams>) =>
    rejectedBy(enqueue(schema.admin, schema.table, { task: "t", ...names }))

  test("task and scope are at most 128 bytes, and queue and key at most 1024 bytes", async () => {
    // "é" takes 2 bytes in UTF-8, and "😀" takes 4.
    expect(
      await enqueueNames({
        task: "é".repeat(64),
        scope: "é".repeat(64),
        queue: "😀".repeat(256),
        key: "a".repeat(1024),
      }),
    ).toBeNull()
    expect(await enqueueNames({ task: "é".repeat(64) + "a" })).toBe("job_task_check")
    expect(await enqueueNames({ scope: "é".repeat(64) + "a" })).toBe("job_scope_check")
    expect(await enqueueNames({ queue: "é".repeat(512) + "a" })).toBe("job_queue_check")
    expect(await enqueueNames({ key: "a".repeat(1025) })).toBe("job_key_check")
  })

  test("names at their limits that do not compress fit the entries of the indexes", async () => {
    // Characters of 4 bytes in UTF-8 from the CJK Unified Ideographs Extension B, U+20000 to U+2A6DF, picked by a seeded generator, so that PostgreSQL cannot compress them.
    const random = createRandom(1)
    const incompressible = (length: number) =>
      Array.from({ length }, () => String.fromCodePoint(random.int(0x20000, 0x2a6df))).join("")
    const names = {
      task: incompressible(32),
      scope: incompressible(32),
      queue: incompressible(256),
      key: incompressible(256),
    }
    const bytes = (name: string) => new TextEncoder().encode(name).length
    expect([names.task, names.scope, names.queue, names.key].map(bytes)).toEqual([
      128, 128, 1024, 1024,
    ])
    expect(await enqueueNames({ ...names, now: T(0) })).toBeNull()
    const [compression] = await query(
      schema.admin,
      `select pg_column_compression(queue) as queue, pg_column_compression(key) as key from ${schema.table.name}`,
    )
    expect(compression).toEqual({ queue: null, key: null })
    // The claim writes the entry of the active queue, and the retry the entries of a pending row again.
    const claimed = await claimQueued(schema.admin, schema.table, {
      worker: "w",
      tasks: [names.task],
      now: T(1),
    })
    if (claimed.kind !== "job") throw new Error(`expected a job, got ${claimed.kind}`)
    expect(
      await fail(schema.database, schema.table, claimed.claim, {
        error: "e",
        backoff: 0,
        now: T(2),
      }),
    ).toBe("retry")
    expect(only(await jobs(schema))).toMatchObject({ ...names, state: "pending" })
  })
})
