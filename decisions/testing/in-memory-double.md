# The test double of the jobs API keeps its jobs in memory

`createTestJobs` of `kern-jobs/testing` is a jobs API over a store in memory: it shares the checks of arguments, options, scope, and the serialization of the payload with `createJobs`, records each enqueue, and keeps for each pending job only its id, task, scope, and key.
A test needs neither a database nor a migrated schema, and checks what the code enqueued, as the code passed it:

```ts
const captured = await testJobs.capture(() => signUp(testJobs, "user@example.com"))
expect(captured.payloadsFor(sendWelcomeEmailTask)).toEqual([{ userId: captured.result.id }])
```

## Reading the job table in the transaction of the test

Helpers that read the pending jobs of the real table, in the transaction that wraps a test, give the real semantics of the core: an enqueue in a transaction that rolls back leaves no job, an enqueue of a key merges by its key mode, a debounce moves the run time, and the constraints of the table reject a key longer than 1024 bytes.
They could also run the pending jobs in that transaction, so that their handlers see its data.
But each test then needs a database with the schema of kern-jobs, and the code under test must enqueue through the client of the transaction of the test; a test that checks what one action enqueued reads a merged pending job rather than the enqueues that made it.

## A pending job that merges by its key mode in memory

The double could keep the payload, the options, and the run time of each pending job and merge an enqueue of its key by the key mode, as graphile-worker-orchid does.
A test could then check the pending job that several enqueues leave.
But the double would repeat the merge of the SQL of the core, such as of debounce windows and attempts, in JavaScript, where it can drift from the core, while the recorded enqueues already show what the code asked for.
