# Terminal failure hook

A hook of a task that runs in the transaction that fails its job for good, so that the application marks its own row, such as an import or a sent document, failed together with the job.
No use case yet: a handler that checks `job.attempt === job.maxAttempts` before it rethrows does the same, except after a crash or a lost lease on the last attempt.

Preliminary: the hook runs on the connection of the fail transaction, after step 2 of [Fail](../architecture/settlement.md#fail), and an error of the hook rolls the transaction back.

Open:

- A job that the reaper fails after its lease has no handler in a worker of its task, so the hook either runs in the worker of the reaper, which needs the task in its manifest, or does not run there.
- Whether the hook receives the error of the handler and the payload, and how long it may hold the row lock.
