# Values in error messages

A check that rejects a value with `RangeError` or `TypeError` formats the value into its message with `String(value)`, which throws `TypeError` for an object without a primitive value, such as `Object.create(null)`, so the caller gets that `TypeError` instead of the error that the check documents, such as `RangeError` for `olderThan` of `deleteFailed`.

Preliminary:

- One helper formats any value for a message without calling its conversions, and every check uses it: the checks of options and backoff in `task/`, of the worker in `worker/`, of counts and storable text in `core/db.ts`, and of ids and `olderThan` in `jobs/jobs.ts`.
- A test per kind of check passes such a value and expects the documented error class.
