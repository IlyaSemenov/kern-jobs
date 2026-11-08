# Worker events

Public events of the worker: starts, completions, requeues after a stop, fenced settlements, and superseded jobs, beside the errors that `onError` gets.

- The events of the runtime carry claims with rows of the table, so the public events need a form of a job of their own, such as the metadata that a handler gets.
- Metrics of completed jobs need them: complete deletes the job, so a query of the table cannot count completions, and a count in the handler includes the runs whose completion the worker fails to record.

Graphile Worker's runner emits events such as `job:success`, `job:error`, and `job:failed` ([docs](https://github.com/graphile/worker/blob/v0.18.0/website/docs/worker-events.md)).
