import { afterEach, beforeEach, expect, test } from "bun:test"

import { createDatabase } from "kern-orm/db"

import { defineSchedule, type Schedule, toWorkerSchedule } from "../../schedule/schedule"
import { enqueue } from "../../sql/enqueue"
import { query } from "../../sql/statement"
import { defineTask } from "../../task/task"
import {
  createTestDriver,
  createTestSchema,
  databaseUrl,
  jobs,
  only,
  type TestSchema,
} from "../../test-harness/database"
import { proxyTo } from "../faults/proxy"

import { claimPlain } from "./claim"
import { evaluateSchedule } from "./schedule"
import { complete } from "./settlement"

let schema: TestSchema
beforeEach(async () => {
  schema = await createTestSchema()
})
afterEach(() => schema.dispose())

const task = defineTask("backup")
const nightly = defineSchedule(task, { cron: { minute: 0, hour: 3 } })
const instant = (time: string) => new Date(`2030-01-02T${time}Z`)
const due = instant("03:00:40")
const run = (schedule: Schedule = nightly, now = due) =>
  evaluateSchedule(schema.database, schema.table, toWorkerSchedule(schedule), { now })

test("catch-up includes the boundary and enqueues at most once, including in the current minute", async () => {
  const scenarios = [
    { id: "first", within: 86400, now: "03:01:00", expected: 0 },
    { id: "skip", previous: "01:01:00", now: "03:01:00", expected: 0 },
    { id: "inside", within: 60, previous: "01:01:00", now: "03:01:00", expected: 1 },
    { id: "outside", within: 59, previous: "01:01:00", now: "03:01:00", expected: 0 },
    { id: "current", within: 7200, previous: "00:01:00", now: "03:00:40", expected: 1 },
  ]
  for (const { id, within, previous, now, expected } of scenarios) {
    const schedule = defineSchedule(defineTask(id), {
      cron: { minute: 0 },
      catchUp: within === undefined ? undefined : { within },
    })
    if (previous) {
      await run(schedule, instant(previous))
    }
    await run(schedule, instant(now))
    expect((await jobs(schema)).filter(job => job.task === id)).toHaveLength(expected)
  }
})

test("catch-up resolves task options at enqueue time and applies schedule overrides", async () => {
  let queue = "initial"
  const resolved: string[] = []
  const task = defineTask("report", { key: "default", runAt: instant("18:00:00") })
    .withPayload<{ region: string }>()
    .withOptions(({ region }) => {
      resolved.push(queue)
      return { queue: `${region}-${queue}`, maxAttempts: 2 }
    })
  const payload = { region: "eu" }
  const schedule = defineSchedule(task, {
    cron: { minute: 0, hour: 3 },
    payload,
    enqueue: { key: null, delay: 10, maxAttempts: 1 },
    catchUp: { within: 3600 },
  })
  await run(schedule, instant("02:00:00"))
  expect(resolved).toEqual([])
  queue = "recovery"
  await run(schedule, instant("03:15:00"))
  expect(resolved).toEqual(["recovery"])
  expect(only(await jobs(schema))).toMatchObject({
    key: null,
    queue: "eu-recovery",
    max_attempts: 1,
    payload: { region: "eu" },
    run_at: instant("03:15:10"),
  })
})

test("an option function that throws rolls the check back, so a later check in the minute enqueues", async () => {
  let failure: Error | undefined = new Error("the options are unavailable")
  const task = defineTask("flaky").withOptions(() => {
    if (failure) throw failure
    return {}
  })
  const schedule = defineSchedule(task, { cron: { minute: 0, hour: 3 } })
  await expect(run(schedule)).rejects.toThrow(failure)
  expect(await jobs(schema)).toEqual([])
  failure = undefined
  await run(schedule, instant("03:00:55"))
  expect(only(await jobs(schema)).task).toBe(task.name)
})

test("a rolled-back first registration leaves no cursor, so catch-up in the next minute recovers nothing", async () => {
  let failure: Error | undefined = new Error("the options are unavailable")
  const task = defineTask("flaky").withOptions(() => {
    if (failure) throw failure
    return {}
  })
  const schedule = defineSchedule(task, { cron: { minute: 0, hour: 3 }, catchUp: { within: 3600 } })
  await expect(run(schedule, instant("03:00:50"))).rejects.toThrow(failure)
  expect(await query(schema.database, `select id from "${schema.name}".schedule`)).toEqual([])
  failure = undefined
  await run(schedule, instant("03:01:10"))
  expect(await jobs(schema)).toEqual([])
})

test("a scheduled enqueue that keeps a pending job consumes the occurrence", async () => {
  const keyed = defineTask("keyed", { key: "shared" }).withPayload<string>()
  const schedule = defineSchedule(keyed, {
    cron: { minute: "*" },
    payload: "scheduled",
    enqueue: { keyMode: "keep" },
  })
  await enqueue(schema.driver, schema.table, {
    task: keyed.name,
    key: "shared",
    payload: "manual",
    now: due,
  })
  await run(schedule)
  expect(only(await jobs(schema))).toMatchObject({ payload: "manual", key_mode: "keep" })
  const claim = only(
    await claimPlain(schema.driver, schema.table, {
      worker: "quick",
      tasks: [keyed.name],
      limit: 1,
      now: due,
    }),
  )
  await complete(schema.db, claim)
  await run(schedule)
  expect(await jobs(schema)).toEqual([])
})

test("three replicas with equivalent calendars enqueue once, including after a fast handler completes and a replica restarts", async () => {
  const replica = () => createDatabase({ driver: schema.createDriver() })
  const text = defineSchedule(task, { cron: "0 3 * * *" })
  await Promise.all(
    [nightly, text, nightly].map(s =>
      evaluateSchedule(replica(), schema.table, toWorkerSchedule(s), { now: due }),
    ),
  )
  expect(await jobs(schema)).toHaveLength(1)
  const claim = only(
    await claimPlain(schema.driver, schema.table, {
      worker: "quick",
      tasks: [task.name],
      limit: 1,
      now: due,
    }),
  )
  await complete(schema.db, claim)
  await evaluateSchedule(replica(), schema.table, toWorkerSchedule(text), { now: due })
  expect(await jobs(schema)).toEqual([])
})

test("a replica skips a locked schedule and a later evaluation can still enqueue it", async () => {
  await run(nightly, instant("02:59:00"))
  const owner = await schema.connect()
  await query(owner, "begin")
  try {
    await query(owner, `select id from "${schema.name}".schedule where id = $1::text for update`, [
      nightly.id,
    ])
    await run()
    expect(await jobs(schema)).toEqual([])
  } finally {
    await query(owner, "rollback")
  }
  await run()
  expect(only(await jobs(schema)).task).toBe(task.name)
})

for (const mode of ["before", "after"] as const) {
  test(`connection lost ${mode} COMMIT: cursor and enqueue recover together`, async () => {
    const { proxy, url } = await proxyTo(databaseUrl())
    const driver = createTestDriver(url, schema.name)
    try {
      proxy.cutNext(/^\s*commit\b/i, mode)
      await expect(
        evaluateSchedule(createDatabase({ driver }), schema.table, toWorkerSchedule(nightly), {
          now: due,
        }),
      ).rejects.toThrow()
      expect(await jobs(schema)).toHaveLength(mode === "before" ? 0 : 1)
      await run()
      expect(await jobs(schema)).toHaveLength(1)
    } finally {
      await driver.close()
      await proxy.close()
    }
  })
}

test("a changed calendar uses its catch-up window only after the last evaluated minute", async () => {
  await run(nightly)
  const changed = defineSchedule(task, {
    cron: { minute: [0, 2] },
    catchUp: { within: 86400 },
  })
  await run(changed, due)
  expect(await jobs(schema)).toHaveLength(1)
  await run(changed, instant("04:01:00"))
  expect(await jobs(schema)).toHaveLength(2)
  await run(changed, instant("04:02:00"))
  expect(await jobs(schema)).toHaveLength(3)
})

test("a changed definition applies to the minutes after the last check, including those before the change", async () => {
  await run(defineSchedule(task, { cron: { minute: 0, hour: 4 } }), instant("02:50:00"))
  const changed = defineSchedule(task, { cron: { minute: 0, hour: 3 }, catchUp: { within: 600 } })
  await run(changed, instant("03:05:00"))
  expect(only(await jobs(schema)).run_at).toEqual(instant("03:05:00"))
})

test("a paused schedule skips the minutes it checks and recovers downtime after resuming", async () => {
  const options = { cron: { minute: 0 }, catchUp: { within: 86400 } } as const
  await run(defineSchedule(task, options), instant("00:01:00"))
  await run(defineSchedule(task, { ...options, enabled: false }), instant("01:00:00"))
  await run(defineSchedule(task, options), instant("01:01:00"))
  expect(await jobs(schema)).toEqual([])
  await run(defineSchedule(task, options), instant("03:01:00"))
  expect(await jobs(schema)).toHaveLength(1)
})
