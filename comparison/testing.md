# Testing

## Enqueues in a test without a database

`createTestJobs` of `kern-jobs/testing` is a jobs API that records each enqueue in memory with its options merged as an enqueue merges them, keeps the pending jobs of keys, and queries the enqueues that an action made by task.

- graphile-worker-tasks, differs: `createTestJobs` of `graphile-worker-tasks/testing` gives an `addJob` that records the identifier, the payload, and the spec of each job, and `capture` queries the jobs that an action added by task, but each `addJob` returns a job of a new id, also with the key of an earlier one ([docs](https://github.com/IlyaSemenov/graphile-worker-tasks/blob/v2.6.1/README.md#testing), [source](https://github.com/IlyaSemenov/graphile-worker-tasks/blob/v2.6.1/src/testing.ts#L71-L79)).
- graphile-worker-orchid, differs: `createTestWorkerUtils` of `graphile-worker-orchid/testing` keeps the pending jobs by key and updates the pending job of a key by the `jobKeyMode` of an enqueue, as `graphile_worker.add_job()` does, and leaves recording to the `addJob` that it is given ([docs](https://github.com/IlyaSemenov/graphile-worker-orchid/blob/v2.5.1/README.md#testing)).

## A handler in a test

`runJob` of `kern-jobs/testing` runs the handler of an implementation with a payload, through JSON, and the metadata of the job that an enqueue of the payload would add.

- graphile-worker-tasks, differs: `task.run(payload, helpers)` of a task without a wrapper runs it with the payload as it is passed, such as with a `Date`, and its default helpers have a synthetic `helpers.job` with `key: null` and `max_attempts: 1`; helpers that the definition or the call gives replace the defaults ([docs](https://github.com/IlyaSemenov/graphile-worker-tasks/blob/v2.6.1/README.md#running-tasks-directly), [source](https://github.com/IlyaSemenov/graphile-worker-tasks/blob/v2.6.1/src/run.ts#L11-L43)).
