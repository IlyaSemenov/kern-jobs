import { expect, test } from "bun:test"

import { checkStorableText } from "./postgres"

test("checkStorableText throws TypeError for U+0000, a lone surrogate, or a value other than a string, and accepts a surrogate pair", () => {
  for (const [value, got] of [
    ["k\0", '"k\\u0000"'],
    // Encoding as UTF-8 replaces both lone surrogates with U+FFFD, so the two keys would share a pending job.
    ["k\ud800", '"k\\ud800"'],
    ["k\udc00", '"k\\udc00"'],
    [5, "5"],
  ] as const) {
    expect(() => checkStorableText("key", value)).toThrow(
      new TypeError(
        `key must be a string that has neither U+0000 nor a lone surrogate, got ${got}`,
      ),
    )
  }
  expect(() => checkStorableText("key", "k😀")).not.toThrow()
})
