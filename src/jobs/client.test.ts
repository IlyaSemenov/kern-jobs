import { describe, expect, test } from "bun:test"

import { queryValues } from "../core/statement"

import { type Executor, fromExecutor } from "./client"

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
