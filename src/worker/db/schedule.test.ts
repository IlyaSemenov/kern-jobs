import { afterEach, beforeEach, describe, expect, mock, test } from "bun:test"

import { type Driver, identifier, isQueryError } from "kern-orm/db"
import { getBackendPid, waitForLock } from "kern-orm/testing"

import { defineSchedule, type Schedule, toWorkerSchedule } from "../../schedule/schedule"
import { enqueue, enqueueItems } from "../../sql/enqueue"
import { toStatementLabel } from "../../sql/statement"
import { defineTask } from "../../task/task"
import {
  createTestDriver,
  createTestSchema,
  databaseUrl,
  getStatementLabel,
  jobs,
  only,
  type TestSchema,
} from "../../test-harness/database"
import { proxyTo } from "../faults/proxy"

import { claimPlain } from "./claim"
import { evaluateSchedule } from "./schedule"
import { complete } from "./settlement"
import * as t from "./types"

let testSchema: TestSchema
beforeEach(async () => {
  testSchema = await createTestSchema()
})
afterEach(() => testSchema.dispose())

const scheduleTableSql = () => identifier(testSchema.name, "schedule")

const task = defineTask("backup")
const nightly = defineSchedule(task, { cron: { minute: 0, hour: 3 } })
const instant = (time: string) => new Date(`2030-01-02T${time}Z`)
const due = instant("03:00:40")
const run = (schedule: Schedule = nightly, now = due) =>
  evaluateSchedule(testSchema.db, testSchema.table, toWorkerSchedule(schedule), { now })

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
    expect((await jobs(testSchema.conn.query)).filter(job => job.task === id)).toHaveLength(
      expected,
    )
  }
})

test("a schedule of a batch task appends its items to the open batch of its key", async () => {
  const batchTask = defineTask("digest", { key: true }).withBatchPayload<string>({ maxItems: 3 })
  const schedule = defineSchedule(batchTask, { cron: { minute: 0, hour: 3 }, payload: ["tick"] })
  await enqueueItems(testSchema.conn, testSchema.table, {
    task: "digest",
    key: "",
    maxItems: 3,
    payload: ["event"],
  })
  await run(schedule)
  expect((await jobs(testSchema.conn.query)).map(job => job.payload)).toEqual([["event", "tick"]])
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
  expect(only(await jobs(testSchema.conn.query))).toMatchObject({
    key: null,
    queue: "eu-recovery",
    maxAttempts: 1,
    payload: { region: "eu" },
    runAt: instant("03:15:10"),
  })
})

/** Returns a database of the test schema that records the labels of the statements of its transactions, without the statements of transaction control and its settings, which carry none. */
function createRecordingDb() {
  const labels: string[] = []
  const recording: Driver = {
    ...testSchema.driver,
    async acquire() {
      const connection = await testSchema.driver.acquire()
      return {
        ...connection,
        execute: statement => {
          const label = getStatementLabel(statement.text)
          if (label !== null) labels.push(label)
          return connection.execute(statement)
        },
      }
    },
  }
  return { db: testSchema.createDb(recording), labels }
}

test("a check labels the registration, the lock, and the advance of the cursor, and its enqueue", async () => {
  const { db, labels } = createRecordingDb()
  await evaluateSchedule(db, testSchema.table, toWorkerSchedule(nightly), { now: due })
  expect(labels).toEqual(
    (["schedule:register", "schedule:lock", "enqueue:one", "schedule:advance"] as const).map(
      toStatementLabel,
    ),
  )
})

test("a check reads the cursor and the clock exactly on a pool whose DateStyle and TimeZone are not the defaults", async () => {
  // In Asia/Kathmandu, these instants of January 2 fall on January 3, which SQL, DMY writes day first.
  const db = testSchema.createDb({ dateStyle: "SQL, DMY", timeZone: "Asia/Kathmandu" })
  const now = new Date("2030-01-02T20:00:40.123Z")
  const covered = defineSchedule(defineTask("covered"), {
    cron: { minute: 59, hour: 19 },
    catchUp: { within: 3600 },
  })
  // Adding one minute to this cursor puts the lower bound one microsecond past the 19:59 occurrence.
  await testSchema.conn.query`
    insert into ${scheduleTableSql()} (id, evaluated_through) values (${covered.id}, '2030-01-02 19:58:00.000001+00')`
  const current = defineSchedule(defineTask("current"), { cron: { minute: 0, hour: 20 } })
  for (const schedule of [covered, current]) {
    await evaluateSchedule(db, testSchema.table, toWorkerSchedule(schedule), { now })
  }
  expect((await jobs(testSchema.conn.query)).map(job => [job.task, job.runAt])).toEqual([
    ["current", now],
  ])
  const cursors = await testSchema.conn.query`
    select id, (extract(epoch from evaluated_through) * 1000)::float8 as cursor from ${scheduleTableSql()} order by id`.select(
    { id: t.text, cursor: t.doublePrecision },
  )
  expect(cursors.map(row => [row.id, row.cursor])).toEqual([
    [covered.id, Date.parse("2030-01-02T20:00:00Z")],
    [current.id, Date.parse("2030-01-02T20:00:00Z")],
  ])
})

test("an option function that throws rolls the check back, so a later check in the minute enqueues", async () => {
  let failure: Error | undefined = new Error("the options are unavailable")
  const task = defineTask("flaky").withOptions(() => {
    if (failure) throw failure
    return {}
  })
  const schedule = defineSchedule(task, { cron: { minute: 0, hour: 3 } })
  await expect(run(schedule)).rejects.toThrow(failure)
  expect(await jobs(testSchema.conn.query)).toEqual([])
  failure = undefined
  await run(schedule, instant("03:00:55"))
  expect(only(await jobs(testSchema.conn.query)).task).toBe(task.name)
})

test("a rolled-back first registration leaves no cursor, so catch-up in the next minute recovers nothing", async () => {
  let failure: Error | undefined = new Error("the options are unavailable")
  const task = defineTask("flaky").withOptions(() => {
    if (failure) throw failure
    return {}
  })
  const schedule = defineSchedule(task, { cron: { minute: 0, hour: 3 }, catchUp: { within: 3600 } })
  await expect(run(schedule, instant("03:00:50"))).rejects.toThrow(failure)
  expect(
    await testSchema.database.query`select id from ${scheduleTableSql()}`.pluck(t.text),
  ).toEqual([])
  failure = undefined
  await run(schedule, instant("03:01:10"))
  expect(await jobs(testSchema.conn.query)).toEqual([])
})

test("a scheduled enqueue that keeps a pending job consumes the occurrence", async () => {
  const keyed = defineTask("keyed", { key: "shared" }).withPayload<string>()
  const schedule = defineSchedule(keyed, {
    cron: { minute: "*" },
    payload: "scheduled",
    enqueue: { keyMode: "keep" },
  })
  await enqueue(testSchema.driver, testSchema.table, {
    task: keyed.name,
    key: "shared",
    payload: "manual",
    now: due,
  })
  await run(schedule)
  expect(only(await jobs(testSchema.conn.query))).toMatchObject({
    payload: "manual",
    keyMode: "keep",
  })
  const claim = only(
    await claimPlain(testSchema.db.$queryOn(testSchema.driver), {
      worker: "quick",
      tasks: [keyed.name],
      limit: 1,
      now: due,
    }),
  )
  await complete(testSchema.db, claim)
  await run(schedule)
  expect(await jobs(testSchema.conn.query)).toEqual([])
})

test("three replicas with equivalent calendars enqueue once, including after a fast handler completes and a replica restarts", async () => {
  const replica = () => testSchema.createDb({})
  const text = defineSchedule(task, { cron: "0 3 * * *" })
  await Promise.all(
    [nightly, text, nightly].map(s =>
      evaluateSchedule(replica(), testSchema.table, toWorkerSchedule(s), { now: due }),
    ),
  )
  expect(await jobs(testSchema.conn.query)).toHaveLength(1)
  const claim = only(
    await claimPlain(testSchema.db.$queryOn(testSchema.driver), {
      worker: "quick",
      tasks: [task.name],
      limit: 1,
      now: due,
    }),
  )
  await complete(testSchema.db, claim)
  await evaluateSchedule(replica(), testSchema.table, toWorkerSchedule(text), { now: due })
  expect(await jobs(testSchema.conn.query)).toEqual([])
})

test("a check whose registration waits for a concurrent one locks the committed row and sets the run_at of its job to the start time of its transaction", async () => {
  const every = defineSchedule(task, { cron: { minute: "*" } })
  const registrar = await testSchema.connect()
  await registrar.query`begin`
  await registrar.query`
    insert into ${scheduleTableSql()} (id, evaluated_through) values (${every.id}, date_trunc('minute', now()) - interval '1 minute')`
  // The default isolation of the pool is serializable, so the check must choose READ COMMITTED itself.
  // A pool of one connection, so that the backend of the check is known.
  const db = testSchema.createDb({ max: 1, defaultIsolation: "serializable" })
  const pid = await getBackendPid(db.$database)
  const check = evaluateSchedule(db, testSchema.table, toWorkerSchedule(every))
  await waitForLock(testSchema.conn, pid)
  const started = await testSchema.conn.query`
    select floor(extract(epoch from xact_start) * 1000)::float8 from pg_stat_activity where pid = ${pid}`.get(
    t.doublePrecision,
  )
  await registrar.query`commit`
  await check
  const job = only(await jobs(testSchema.conn.query))
  expect(job.runAt.getTime()).toBe(started)
  const cursor = await testSchema.conn.query`
    select (extract(epoch from evaluated_through) * 1000)::float8 from ${scheduleTableSql()}`.get(
    t.doublePrecision,
  )
  expect(cursor).toBe(Math.floor(job.runAt.getTime() / 60000) * 60000)
})

test("a replica skips a locked schedule and a later evaluation can still enqueue it", async () => {
  await run(nightly, instant("02:59:00"))
  const owner = await testSchema.connect()
  await owner.query`begin`
  try {
    await owner.query`select id from ${scheduleTableSql()} where id = ${nightly.id} for update`
    await run()
    expect(await jobs(testSchema.conn.query)).toEqual([])
  } finally {
    await owner.query`rollback`
  }
  await run()
  expect(only(await jobs(testSchema.conn.query)).task).toBe(task.name)
})

for (const mode of ["before", "after"] as const) {
  test(`connection lost ${mode} COMMIT: cursor and enqueue recover together`, async () => {
    const { proxy, url } = await proxyTo(databaseUrl())
    const driver = createTestDriver(url, testSchema.name)
    try {
      proxy.cutNext(/^\s*commit\b/i, mode)
      await expect(
        evaluateSchedule(testSchema.createDb(driver), testSchema.table, toWorkerSchedule(nightly), {
          now: due,
        }),
      ).rejects.toThrow()
      expect(await jobs(testSchema.conn.query)).toHaveLength(mode === "before" ? 0 : 1)
      await run()
      expect(await jobs(testSchema.conn.query)).toHaveLength(1)
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
  expect(await jobs(testSchema.conn.query)).toHaveLength(1)
  await run(changed, instant("04:01:00"))
  expect(await jobs(testSchema.conn.query)).toHaveLength(2)
  await run(changed, instant("04:02:00"))
  expect(await jobs(testSchema.conn.query)).toHaveLength(3)
})

test("a changed definition applies to the minutes after the last check, including those before the change", async () => {
  await run(defineSchedule(task, { cron: { minute: 0, hour: 4 } }), instant("02:50:00"))
  const changed = defineSchedule(task, { cron: { minute: 0, hour: 3 }, catchUp: { within: 600 } })
  await run(changed, instant("03:05:00"))
  expect(only(await jobs(testSchema.conn.query)).runAt).toEqual(instant("03:05:00"))
})

test("a paused schedule skips the minutes it checks and recovers downtime after resuming", async () => {
  const options = { cron: { minute: 0 }, catchUp: { within: 86400 } } as const
  await run(defineSchedule(task, options), instant("00:01:00"))
  await run(defineSchedule(task, { ...options, enabled: false }), instant("01:00:00"))
  await run(defineSchedule(task, options), instant("01:01:00"))
  expect(await jobs(testSchema.conn.query)).toEqual([])
  await run(defineSchedule(task, options), instant("03:01:00"))
  expect(await jobs(testSchema.conn.query)).toHaveLength(1)
})

describe("scopes", () => {
  const metrics = defineTask("metrics.recalculate", { scoped: true })
  const cron = { minute: 0, hour: 3 } as const
  const scopesOfJobs = async () => (await jobs(testSchema.conn.query)).map(job => job.scope)
  /** Returns the cursor of a schedule in milliseconds since the epoch, or `undefined` for a schedule without a row. */
  const cursorOf = async (id: string) =>
    (
      await testSchema.conn.query`
        select (extract(epoch from evaluated_through) * 1000)::float8 from ${scheduleTableSql()} where id = ${id}`.pluck(
        t.doublePrecision,
      )
    )[0]

  test("a schedule of a scoped task enqueues once in each of its scopes, in their sorted order, by one statement", async () => {
    const schedule = defineSchedule(metrics, { cron, scopes: ["b", "c", "a", "b"] })
    const { db, labels } = createRecordingDb()
    await evaluateSchedule(db, testSchema.table, toWorkerSchedule(schedule), { now: due })
    expect(labels).toEqual(
      (["schedule:register", "schedule:lock", "enqueue:scopes", "schedule:advance"] as const).map(
        toStatementLabel,
      ),
    )
    await run(schedule, instant("03:00:55"))
    expect((await jobs(testSchema.conn.query)).map(job => [job.scope, job.runAt])).toEqual([
      ["a", due],
      ["b", due],
      ["c", due],
    ])
  })

  test("a scheduled enqueue merges into the pending job of the key of its own scope", async () => {
    const keyed = defineTask("keyed", { scoped: true, key: true }).withPayload<string>()
    const schedule = defineSchedule(keyed, { cron, payload: "scheduled", scopes: ["a", "b"] })
    await enqueue(testSchema.driver, testSchema.table, {
      task: keyed.name,
      scope: "b",
      key: "",
      payload: "manual",
      now: instant("02:00:00"),
    })
    await run(schedule)
    expect(
      (await jobs(testSchema.conn.query)).map(job => [job.scope, job.payload, job.runAt]),
    ).toEqual([
      ["b", "scheduled", due],
      ["a", "scheduled", due],
    ])
  })

  test("an enqueue that PostgreSQL rejects in one scope leaves no job of another scope and no cursor", async () => {
    const keyed = defineTask("keyed", { scoped: true, key: true })
    const schedule = defineSchedule(keyed, { cron, scopes: ["a", "b", "c"] })
    // A pending batch of the key, into which a job that is not a batch cannot merge.
    await enqueueItems(testSchema.conn, testSchema.table, {
      task: keyed.name,
      scope: "b",
      key: "",
      maxItems: 2,
      payload: ["item"],
    })
    await expect(run(schedule)).rejects.toThrow("is a batch")
    expect(await scopesOfJobs()).toEqual(["b"])
    expect(await cursorOf(schedule.id)).toBeUndefined()
  })

  test("the function of the scopes runs only for a check that finds an occurrence due, whose jobs take the scopes that it returns", async () => {
    let tenants = ["b", "a"]
    const scopes = mock(async () => tenants)
    const options = { cron, scopes, catchUp: { within: 3600 } } as const
    await run(defineSchedule(metrics, options), instant("02:59:30"))
    await run(defineSchedule(metrics, { ...options, enabled: false }), instant("03:00:10"))
    await run(defineSchedule(metrics, options), instant("03:00:40"))
    expect(scopes).not.toHaveBeenCalled()
    expect(await scopesOfJobs()).toEqual([])
    // The paused minute stays skipped, and the next occurrence takes the scopes of its own check.
    await run(defineSchedule(metrics, options), instant("03:01:00"))
    expect(scopes).not.toHaveBeenCalled()
    const hourly = defineSchedule(metrics, { ...options, cron: { minute: 0 } })
    await run(hourly, instant("04:00:05"))
    expect(scopes).toHaveBeenCalledTimes(1)
    expect(await scopesOfJobs()).toEqual(["a", "b"])
    // An empty list consumes the occurrence.
    tenants = []
    await run(hourly, instant("05:00:05"))
    expect(scopes).toHaveBeenCalledTimes(2)
    expect(await scopesOfJobs()).toEqual(["a", "b"])
    expect(await cursorOf(hourly.id)).toBe(instant("05:00:00").getTime())
    tenants = ["c"]
    await run(hourly, instant("05:00:20"))
    expect(scopes).toHaveBeenCalledTimes(2)
  })

  test("a check with a function of the scopes labels the statements of its two transactions", async () => {
    const schedule = defineSchedule(metrics, { cron, scopes: () => ["a", "b"] })
    const { db, labels } = createRecordingDb()
    await evaluateSchedule(db, testSchema.table, toWorkerSchedule(schedule), { now: due })
    expect(labels).toEqual(
      (
        [
          "schedule:register",
          "schedule:lock",
          "schedule:register",
          "schedule:lock",
          "enqueue:scopes",
          "schedule:advance",
        ] as const
      ).map(toStatementLabel),
    )
  })

  test("a check calls the function of the scopes once for the single enqueue of a catch-up window of several occurrences", async () => {
    const scopes = mock(() => ["a", "b"])
    const schedule = defineSchedule(metrics, {
      cron: { minute: { every: 10 }, hour: 3 },
      scopes,
      catchUp: { within: 3600 },
    })
    await run(schedule, instant("02:59:30"))
    await run(schedule, instant("03:35:00"))
    expect(scopes).toHaveBeenCalledTimes(1)
    expect(await scopesOfJobs()).toEqual(["a", "b"])
  })

  test("a check resolves the options of the task once for all its scopes, and not at all for no scopes", async () => {
    let tenants = ["a", "b", "c"]
    const keyFor = mock(() => "k")
    const optionsFor = mock(() => ({ queue: "q" }))
    const keyed = defineTask("keyed", { scoped: true }).withKey(keyFor).withOptions(optionsFor)
    const schedule = defineSchedule(keyed, { cron: { minute: 0 }, scopes: () => tenants })
    await run(schedule, instant("03:00:40"))
    expect([keyFor.mock.calls.length, optionsFor.mock.calls.length]).toEqual([1, 1])
    expect((await jobs(testSchema.conn.query)).map(job => [job.scope, job.key, job.queue])).toEqual(
      [
        ["a", "k", "q"],
        ["b", "k", "q"],
        ["c", "k", "q"],
      ],
    )
    tenants = []
    await run(schedule, instant("04:00:40"))
    expect([keyFor.mock.calls.length, optionsFor.mock.calls.length]).toEqual([1, 1])
    expect(await cursorOf(schedule.id)).toBe(instant("04:00:00").getTime())
  })

  test("a function of the scopes that throws, or returns an invalid scope, enqueues nothing and leaves the occurrence to a later check, also of a new schedule", async () => {
    let result: () => unknown = () => {
      throw new Error("the registry is unavailable")
    }
    const schedule = defineSchedule(metrics, {
      cron,
      scopes: () => result() as string[],
      catchUp: { within: 600 },
    })
    await expect(run(schedule)).rejects.toThrow("the registry is unavailable")
    result = () => ["a", ""]
    await expect(run(schedule, instant("03:00:45"))).rejects.toThrow(RangeError)
    result = () => "a"
    await expect(run(schedule, instant("03:00:50"))).rejects.toThrow(
      "scopes of schedule metrics.recalculate must be an array of scopes",
    )
    expect(await scopesOfJobs()).toEqual([])
    // The first check registered the schedule before it called the function, so catch-up recovers the occurrence.
    expect(await cursorOf(schedule.id)).toBe(instant("02:59:00").getTime())
    result = () => ["a"]
    await run(schedule, instant("03:02:00"))
    expect(await scopesOfJobs()).toEqual(["a"])
    expect(await cursorOf(schedule.id)).toBe(instant("03:02:00").getTime())
  })

  for (const mode of ["before", "after"] as const) {
    test(`connection lost ${mode} the COMMIT of the enqueue in the scopes of a function: the jobs of every scope and the cursor recover together`, async () => {
      const { proxy, url } = await proxyTo(databaseUrl())
      const driver = createTestDriver(url, testSchema.name)
      try {
        const schedule = defineSchedule(metrics, {
          cron,
          scopes: () => {
            // The transaction that found the enqueue due has committed.
            proxy.cutNext(/^\s*commit\b/i, mode)
            return ["a", "b", "c"]
          },
        })
        await expect(
          evaluateSchedule(
            testSchema.createDb(driver),
            testSchema.table,
            toWorkerSchedule(schedule),
            {
              now: due,
            },
          ),
        ).rejects.toThrow()
        expect(await scopesOfJobs()).toEqual(mode === "before" ? [] : ["a", "b", "c"])
        expect(await cursorOf(schedule.id)).toBe(
          instant(mode === "before" ? "02:59:00" : "03:00:00").getTime(),
        )
        await run(
          defineSchedule(metrics, { cron, scopes: () => ["a", "b", "c"] }),
          instant("03:00:55"),
        )
        expect(await scopesOfJobs()).toEqual(["a", "b", "c"])
      } finally {
        await driver.close()
        await proxy.close()
      }
    })
  }

  test("a check drops the scopes that its function returned once a replica has checked the minute meanwhile", async () => {
    const replica = defineSchedule(metrics, { cron, scopes: ["a"] })
    const schedule = defineSchedule(metrics, {
      cron,
      scopes: async () => {
        await run(replica)
        return ["a", "b"]
      },
    })
    await run(schedule)
    expect(await scopesOfJobs()).toEqual(["a"])
  })

  test("replicas whose functions of the scopes run for one occurrence call both, and only the first to lock the row with its list enqueues it", async () => {
    let calls = 0
    const called = Promise.withResolvers<void>()
    const define = (list: Promise<string[]>) =>
      defineSchedule(metrics, {
        cron,
        scopes: () => {
          if (++calls === 2) called.resolve()
          return list
        },
      })
    const first = Promise.withResolvers<string[]>()
    const second = Promise.withResolvers<string[]>()
    const firstCheck = run(define(first.promise), instant("03:00:40"))
    const secondCheck = run(define(second.promise), instant("03:00:45"))
    await called.promise
    second.resolve(["b"])
    await secondCheck
    first.resolve(["a"])
    await firstCheck
    expect(await scopesOfJobs()).toEqual(["b"])
  })

  for (const within of [undefined, 3600]) {
    test(`a replica that checks a later minute while the function of the scopes runs ${within ? "enqueues the occurrence with its own scopes by the catch-up window" : "skips the occurrence without a catch-up window"}, and the check drops its list`, async () => {
      const options = { cron, catchUp: within === undefined ? undefined : { within } }
      const replica = defineSchedule(metrics, { ...options, scopes: ["r"] })
      const schedule = defineSchedule(metrics, {
        ...options,
        scopes: async () => {
          await run(replica, instant("03:01:05"))
          return ["a"]
        },
      })
      await run(schedule, instant("03:00:50"))
      expect(await scopesOfJobs()).toEqual(within ? ["r"] : [])
      expect(await cursorOf(metrics.name)).toBe(instant("03:01:00").getTime())
    })
  }

  test("a check enqueues the occurrence that a replica left due while its function ran, which the next check would find past the catch-up window", async () => {
    const options = { cron: { minute: [58, 59], hour: 2 }, catchUp: { within: 65 } } as const
    const called = Promise.withResolvers<void>()
    const tenants = Promise.withResolvers<string[]>()
    // The replica waits for the scopes of the 02:58 occurrence.
    const replica = run(
      defineSchedule(metrics, {
        ...options,
        scopes: () => {
          called.resolve()
          return tenants.promise
        },
      }),
      instant("02:58:30"),
    )
    await called.promise
    // This check finds an enqueue due under a cursor before 02:58, and the replica enqueues 02:58 while its function runs, which leaves it 02:59.
    await run(
      defineSchedule(metrics, {
        ...options,
        scopes: async () => {
          tenants.resolve(["a"])
          await replica
          return ["a"]
        },
      }),
      instant("03:00:03"),
    )
    expect((await jobs(testSchema.conn.query)).map(job => job.runAt)).toEqual([
      instant("02:58:30"),
      instant("03:00:03"),
    ])
    expect(await cursorOf(metrics.name)).toBe(instant("03:00:00").getTime())
  })

  test("a check that has its scopes waits for a replica that holds the row, and enqueues the occurrence that the replica leaves due", async () => {
    await testSchema.conn.query`
      insert into ${scheduleTableSql()} (id, evaluated_through) values (${metrics.name}, '2030-01-02 02:57:00+00')`
    const replica = await testSchema.connect()
    // A pool of one connection, so that the backend of the check is known.
    const db = testSchema.createDb({ max: 1 })
    const pid = await getBackendPid(db.$database)
    const schedule = defineSchedule(metrics, {
      cron: { minute: [58, 59], hour: 2 },
      catchUp: { within: 65 },
      scopes: async () => {
        // The replica takes the row for its enqueue of 02:58 while the function runs.
        await replica.query`begin`
        await replica.query`select id from ${scheduleTableSql()} where id = ${metrics.name} for update`
        return ["a"]
      },
    })
    const check = evaluateSchedule(db, testSchema.table, toWorkerSchedule(schedule), {
      now: instant("03:00:03"),
    })
    await waitForLock(testSchema.conn, pid)
    await replica.query`
      update ${scheduleTableSql()} set evaluated_through = '2030-01-02 02:58:00+00' where id = ${metrics.name}`
    await replica.query`commit`
    await check
    // A check at 03:00:18 would find 02:59 past the catch-up window.
    expect((await jobs(testSchema.conn.query)).map(job => [job.scope, job.runAt])).toEqual([
      ["a", instant("03:00:03")],
    ])
    expect(await cursorOf(schedule.id)).toBe(instant("03:00:00").getTime())
  })

  test("a check whose scopes wait for a replica that holds the row past the lock timeout fails with lock_not_available and leaves the occurrence due", async () => {
    const replica = await testSchema.connect()
    const schedule = defineSchedule(metrics, {
      cron,
      scopes: async () => {
        await replica.query`begin`
        await replica.query`select id from ${scheduleTableSql()} where id = ${metrics.name} for update`
        return ["a"]
      },
    })
    try {
      const error = await run(schedule).catch((error: unknown) => error)
      expect(isQueryError(error, "lock_not_available")).toBe(true)
    } finally {
      await replica.query`rollback`
    }
    expect(await scopesOfJobs()).toEqual([])
    expect(await cursorOf(schedule.id)).toBe(instant("02:59:00").getTime())
  })

  test("a check holds no transaction while the function of its scopes runs, and then checks as of the time before the call", async () => {
    const every = defineSchedule(metrics, {
      cron: { minute: "*" },
      scopes: async () => {
        // A pool of one connection would wait for itself if the check held its connection.
        called = await db.$query`select (extract(epoch from clock_timestamp()) * 1000)::float8`.get(
          t.doublePrecision,
        )
        return ["a"]
      },
    })
    let called = 0
    const db = testSchema.createDb({ max: 1 })
    await evaluateSchedule(db, testSchema.table, toWorkerSchedule(every))
    const job = only(await jobs(testSchema.conn.query))
    expect(job.runAt.getTime()).toBeLessThanOrEqual(called)
    expect(await cursorOf(every.id)).toBe(Math.floor(job.runAt.getTime() / 60000) * 60000)
  })

  test("a check bounds its wait for a lock of the schedule table, and calls the function of the scopes only after it", async () => {
    const scopes = mock(() => ["a"])
    const schedule = defineSchedule(metrics, { cron, scopes })
    const owner = await testSchema.connect()
    await owner.query`begin`
    try {
      await owner.query`lock table ${scheduleTableSql()} in access exclusive mode`
      const error = await run(schedule).catch((error: unknown) => error)
      expect(isQueryError(error, "lock_not_available")).toBe(true)
    } finally {
      await owner.query`rollback`
    }
    expect(scopes).not.toHaveBeenCalled()
    await run(schedule)
    expect(await scopesOfJobs()).toEqual(["a"])
  })

  test("a check that may no longer proceed once the function of its scopes returns takes no connection and writes nothing", async () => {
    let stopped = false
    let acquired = 0
    const counting: Driver = {
      ...testSchema.driver,
      acquire() {
        acquired++
        return testSchema.driver.acquire()
      },
    }
    const schedule = defineSchedule(metrics, {
      cron,
      scopes: () => {
        stopped = true
        return ["a"]
      },
    })
    await evaluateSchedule(
      testSchema.createDb(counting),
      testSchema.table,
      toWorkerSchedule(schedule),
      { now: due, proceed: () => !stopped },
    )
    // The transaction that found the enqueue due took the only connection.
    expect([stopped, acquired]).toEqual([true, 1])
    expect(await scopesOfJobs()).toEqual([])
  })
})
