import { afterAll, expect, test } from "bun:test"

import { postgresJsDriver } from "kern-orm/db/postgres-js"
import postgres from "postgres"

import { databaseUrl } from "../../test-harness/database"

import { query } from "./query"

const driver = postgresJsDriver(postgres(databaseUrl(), { max: 1 }))
afterAll(() => driver.close())

test("returns rows by the names of their columns", async () => {
  expect(
    await query(driver, `select * from (values (1, null), (2, 'b')) as v(id, label) order by id`),
  ).toEqual([
    { id: "1", label: null },
    { id: "2", label: "b" },
  ])
})

test("keeps the last of several columns of one name", async () => {
  expect(await query(driver, `select 'x' as a, null as a`)).toEqual([{ a: null }])
})
