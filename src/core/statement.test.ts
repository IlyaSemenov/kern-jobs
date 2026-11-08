import { afterAll, describe, expect, test } from "bun:test"

import { postgresJsDriver } from "kern-db/postgres-js"
import postgres from "postgres"

import { type Executor, fromExecutor, query, queryValues } from "./statement"
import { databaseUrl } from "./testing"

const driver = postgresJsDriver(postgres(databaseUrl(), { max: 1 }))
afterAll(() => driver.close())

test("encodes parameters as text that the SQL casts", async () => {
  const at = new Date(Date.UTC(2030, 0, 1, 0, 0, 0, 123))
  const rows = await query(
    driver,
    `select $1::text::int + 1 as n, $2::text::boolean as b, $3::text::timestamptz = '2030-01-01 00:00:00.123+00' as at,
            $4::text::text[] as arr, $5::text is null as missing, $6::text is null as omitted, $7::text as s`,
    [41, true, at, ["a", 'b"c', "d\\e"], null, undefined, "x"],
  )
  expect(rows).toEqual([
    { n: "42", b: "t", at: "t", arr: '{a,"b\\"c","d\\\\e"}', missing: "t", omitted: "t", s: "x" },
  ])
})

test("returns rows by the names of their columns", async () => {
  expect(
    await query(driver, `select * from (values (1, null), (2, 'b')) as v(id, label) order by id`),
  ).toEqual([
    { id: "1", label: null },
    { id: "2", label: "b" },
  ])
})

test("returns rows as arrays of values by position", async () => {
  expect(await queryValues(driver, `select $1::text as a, null as a`, ["x"])).toEqual([["x", null]])
})

test("rejects a number parameter that is not finite", async () => {
  await expect(query(driver, `select $1::text::float8`, [Number.NaN])).rejects.toBeInstanceOf(
    TypeError,
  )
})

describe("fromExecutor", () => {
  test("passes the statement and reads the values of rows by position, whatever the names of the columns", async () => {
    const calls: unknown[] = []
    const executor: Executor = {
      async query(text, values) {
        calls.push([text, values])
        return { rows: [{ renamed: "a", other: null, missing: undefined }, ["b", null]] }
      },
    }
    expect(await queryValues(fromExecutor(executor), "select", ["x", null])).toEqual([
      ["a", null, null],
      ["b", null],
    ])
    expect(calls).toEqual([["select", ["x", null]]])
  })

  test("rejects a value that the client parsed", async () => {
    const executor: Executor = { query: async () => ({ rows: [{ id: 1 }] }) }
    await expect(queryValues(fromExecutor(executor), "select")).rejects.toBeInstanceOf(TypeError)
  })
})
