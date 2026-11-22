import { afterAll, expect, test } from "bun:test"

import { postgresJsDriver } from "kern-orm/db/postgres-js"
import postgres from "postgres"

import { databaseUrl } from "../test-harness/database"

import { queryValues } from "./statement"

const driver = postgresJsDriver(postgres(databaseUrl(), { max: 1 }))
afterAll(() => driver.close())

test("encodes parameters as text that the SQL casts", async () => {
  const at = new Date(Date.UTC(2030, 0, 1, 0, 0, 0, 123))
  const rows = await queryValues(
    driver,
    `select $1::text::int + 1, $2::text::boolean, $3::text::timestamptz = '2030-01-01 00:00:00.123+00',
            $4::text::text[], $5::text is null, $6::text is null, $7::text`,
    [41, true, at, ["a", 'b"c', "d\\e"], null, undefined, "x"],
  )
  expect(rows).toEqual([["42", "t", "t", '{a,"b\\"c","d\\\\e"}', "t", "t", "x"]])
})

test("returns rows as arrays of values by position", async () => {
  expect(await queryValues(driver, `select $1::text as a, null as a`, ["x"])).toEqual([["x", null]])
})

test("rejects a number parameter that is not finite", async () => {
  await expect(queryValues(driver, `select $1::text::float8`, [Number.NaN])).rejects.toBeInstanceOf(
    TypeError,
  )
})
