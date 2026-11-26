# Producer

An application defines its tasks with `defineTask` and enqueues their jobs with `jobs.enqueue`, `jobs.enqueueMany`, or `jobs.enqueueItems` of `createJobs`, through its own database client, through which `jobs` also retries and deletes jobs ([Administration](administration.md)).
`createJobs` builds the jobs API with `createJobsApi` of `producer/jobs.ts`, which merges and checks the options, checks the arguments, and resolves the scope of each call, over a store that runs enqueue and administration statements on the application client; the [test double](testing.md) builds it over a store in memory.

## Task definitions

- A task has a name, which jobs store as `task`, the type of its payload, the settings `lease`, `backoff`, `scoped`, and `maxItems`, and the options of its enqueues: those that `defineTask` takes beside the settings, the key that the function of `withKey` returns for a payload, and the other options that the function of `withOptions` returns for it.
- Producers and workers import the same definitions, which hold no code of handlers; a handler joins its task in an implementation ([Worker](worker.md#tasks)).
- The name keeps its literal type, which `withPayload<P>()` preserves since an explicit type argument of `defineTask` would widen it.
- `withOptions` takes only a function of the payload, so that each static option has one place, `defineTask`; it comes after `withPayload`, whose type the function takes ([decision](../decisions/tasks/definition-shape.md)).
- `withKey` takes a function that returns the key of a payload, or `null` for none, or the key of an item of a batch task, and the options of `withOptions` give no key, so that a function of the payload gives the key in one place for tasks of both kinds ([decision](../decisions/tasks/with-key.md)); `withKey` comes before `withOptions`, whose task takes no `withKey`.
  The task keeps the functions as `keyFor` and `optionsFor`.
- `withBatchPayload<Item>({ maxItems })` makes a `BatchTask<Name, Item>`, whose payload is `Item[]`, and takes only `withKey` after it: a batch task has no `withOptions`, since options computed from the array of an enqueue would apply to the whole batch, which holds the items of other enqueues too; it throws `TypeError` for options that are not an object and for `keyMode: "keep"` of the task, which would drop the items of the enqueues after the first, and `RangeError` for a `maxItems` that `checkCount` rejects, and `maxItems` is a setting of the task, which the failure task of its implementation does not take.
- `Task` has `maxItems?: undefined` and `BatchTask` `maxItems: number`, so that neither is assignable to the other, also a task of `withPayload<Item[]>()` to a `BatchTask`, and the API of each kind takes only its own.
  Both extend `TaskBase`, which the APIs of a task of either kind take, such as `deletePending`, the testing API, and the internals; `defineSchedule` has an overload of each kind, whose overrides of a batch task are `BatchEnqueueOverrides`.
  `keyFor` belongs to `Task` and `BatchTask`, not `TaskBase`, since it takes a payload of one and an item of the other.
  `AnyTask`, the union of the two, is narrowed by `maxItems === undefined`, for code that takes a task of either kind and calls the API of its kind.
- `implement` gives the `task` of an implementation the polymorphic `this` type, the type of the task that the method is read from, so that the API of its kind takes it; `TaskImplementation` without its fourth type argument has a `TaskBase`.
- `keyFor`, `optionsFor`, and `implement` of a task are methods, so that a task of any payload is a `TaskBase<string, unknown>`, and the handler of an implementation is a method, so that an implementation of any payload is a `TaskImplementation<string, unknown>`.
- A task without `withPayload` has a `void` payload; an enqueue may omit a payload whose type admits `undefined`, and the job stores SQL NULL, which its handler gets as `undefined`, while a `null` payload stays JSON `null`.
- `lease`, `backoff`, and [`scoped`](#scope) are not options of an enqueue.
  The worker reads `lease` and `backoff` from the code of the task, the lease when it claims a job, before it reads the payload, and the backoff when a job fails, so the row stores neither, and an enqueue cannot override them ([decision](../decisions/tasks/worker-settings.md)): the types of the options of `withOptions` and of overrides reject the three, and an enqueue throws `TypeError` for one that a JavaScript caller passes.
- A lease is a number of seconds above 0 up to 10^12, about 31,700 years, a bound well within the intervals and timestamps of PostgreSQL, which rejects a claim whose lease is out of the range of an interval.
- A name of a task is at most 128 bytes in UTF-8, the [limit of the column](data-model.md#columns).
- `defineTask` throws `TypeError` for a name that is not [storable text](README.md#terms), options that [`readOptions`](#options) rejects, a `backoff` that is not a function, or a `scoped` that is not a boolean, and `RangeError` for a longer name, a name that ends with `:failure`, which names [failure tasks](failure-jobs.md#failure-tasks), or a `lease` outside its range; `withKey`, `withOptions`, and `implement` throw `TypeError` for an argument that is not a function, and `implement` also for options that are not an object and an `onFailure` that is neither a function nor an object of a function `handler`, and, with `onFailure`, the errors of a name for the name of the [failure task](failure-jobs.md#failure-tasks), `TypeError` for a backoff that is not a function, and `RangeError` for a `maxAttempts` that is not a count.

## Options

An enqueue merges the options of `defineTask`, the key of `withKey` and the options of `withOptions` for its payload, and its overrides, in this order, checks the result, and passes it to the [enqueue](enqueue.md) of the core:

- `readOptions` of `task/options.ts` copies each object of options, of `defineTask`, of `withOptions`, and the overrides of an enqueue or a schedule, before anything else uses it: it reads each field once by property access, so an inherited property and a getter of a class count as an own property does, and the precedence of the key, the checks, and the merge use the same values; `enqueueMany` reads its overrides once per call, also without payloads.
  It throws `TypeError` for options other than `undefined` that are `null`, an array, or not an object, and drops the fields that it does not read.
  For a batch task, `readOverrides` also rejects an own enumerable property of the overrides other than `key` whose value is not `undefined`, such as a misspelled `key`, which would put the items into the batch of another key.
- An override of the key replaces the key of `withKey` without calling `withKey`, as for a task without it; the options of `withOptions` that give a key throw `TypeError`, since `withKey` gives it, and a key of `withKey` that is neither storable text nor, for a task that is not a batch task, `null` throws `TypeError`.

- Each layer replaces the options it gives with a value other than `undefined`, so an option that a function or a wrapper passes through as `undefined` keeps the option of the layers before it.
- `key: null` and `queue: null` remove the key and the queue of the layers before; every other option has a value that equals its absence, such as `delay: 0` or `keyMode: "replace"`.
- `key: true` is the empty key, `""`, which `toStoredKey` of `task/options.ts` gives after the merge, so that the enqueues of a task that merge into one pending job per scope need no constant key; an enqueue and the test double record `""`, and the filter of `deletePending` takes `true` too ([Administration](administration.md#delete-pending-jobs)).
- `delay` and `runAt` exclude each other: a layer that gives one drops the other of the layers before it, and both left after the merge, such as from one layer, throw `TypeError`.
- `keyMode: "debounce"` takes `delay` and `maxWait`, and no `runAt`, so that a stream of events cannot postpone the run time of a job without a bound.
- After the merge, an option that the key mode does not read, such as `maxWait` outside `debounce` or `resetAttempts` with `keep`, is ignored and not checked, since a later layer cannot remove it; the types reject such combinations within one layer.
- The types of every layer admit `undefined` in every optional field also under `exactOptionalPropertyTypes`, so that a wrapper can pass its optional parameters through; the options of `defineTask` and `withOptions` that give `keyMode: "debounce"` give `delay` and `maxWait` too, while overrides may inherit either.
- The checks throw `TypeError` for a wrong combination, a task name that is not [storable text](README.md#terms), or a key that is neither `null`, `true`, nor storable text, or a queue that is neither `null` nor storable text, and `RangeError` for a value outside its range, before the client is called; the check constraints of the table reject a key or a queue longer than 1024 bytes.
  Each enqueue checks the task name again, since the task may come from an older version of kern-jobs, whose `defineTask` accepted such a name.
- `delay` and `maxWait` are numbers of seconds from 0 to 10^12, about 31,700 years, a bound well within the intervals and timestamps of PostgreSQL.
  `isDelay` of `task/delay.ts` checks this range, which delays of retries share.
  A run time in the past takes `runAt` rather than a negative `delay`.
- `runAt` is a `Date` in the years 1 to 9999: a statement sends it as `toISOString` gives it, which PostgreSQL reads only for these years.
- `maxAttempts` is an integer from 1 to 2^31 − 1, the largest `int`, which `checkCount` of `limits.ts` checks, as it checks the counts of the worker.
- `delay` counts from `statement_timestamp()` of the enqueue in SQL, not from the clock of the producer.

## Batch tasks

`jobs.enqueueItems` passes the items of an enqueue of a batch task with `maxItems` of the task to the core, which appends them as a [batch](enqueue.md#batches) ([decision](../decisions/enqueue/items-method.md)):

- It merges the options of `defineTask`, the key of `withKey`, which it calls once for each item unless the overrides give a key, and the overrides, `BatchEnqueueOverrides`, which give only the key, since the other options apply to the whole batch ([decision](../decisions/tasks/with-key.md)); every job that its items fill takes them.
- The checks throw `TypeError` for an override other than `key`, before `withKey` is called, for items that are not an array, for items whose keys of `withKey` differ, for items without a key, since the key identifies the batch, and for `keyMode: "keep"`, which a task of the shape of a batch task can give past `withBatchPayload`, before the client is called.
- It resolves to `{ id, created }` of each job that took items, in the order of the items, and an empty array needs no key, runs no statement, and resolves to `[]`.
- One call appends to one key; appends to several keys in one statement are not supported, and a transaction of several calls makes them all or none.
- `jobs.enqueue` and `jobs.enqueueMany` reject a batch task, and `jobs.enqueueItems` a task that is not one, with `TypeError`, beside the types, which a cast or JavaScript bypasses.

## Many jobs

`jobs.enqueueMany` enqueues a job of one task for each payload of an array, with the same overrides, by the [enqueue of many jobs](enqueue.md#many-jobs) of the core, all or none ([decision](../decisions/enqueue/many-of-one-task.md)):

- Each payload merges and checks its options as an enqueue does, and the call checks those of every payload, the keys, and the scope, which it resolves once, before it calls the client; `createJobs` serializes every payload before it calls the function of the client.
- The call rejects with `TypeError` if `payloads` is not an array, and with `RangeError` if two payloads resolve to the same stored key, so `key: true` and `""` count as one.
- Without payloads, it checks the name of the task, the overrides by `checkEnqueueOverrides` of `task/options.ts`, and the scope, and resolves to `[]` without calling `withKey`, `withOptions`, the store, or the client.
- The store of the jobs API takes only an enqueue of the jobs of one task and scope, so `jobs.enqueue` is an enqueue of one job, and the test double has one enqueue.

## Client

`createJobs` takes the client of the application as its first argument, and its options as its second:

- an executor, an object with `query(text, values)` that resolves to `{ rows }`, such as a `pg.Pool` or a `pg.PoolClient` ([decision](../decisions/drivers/executor.md));
- a client of postgres.js, `sql` or a transaction, whose `unsafe` kern-jobs calls as the `query` of an executor;
- a function that returns one of them, which `jobs` calls for each statement that it runs, such as of an enqueue, so that the statement runs in the transaction that the application holds at the call, such as one in `AsyncLocalStorage`;
- a `Database` of kern-db or a `Db` of kern-orm, whose `execute` runs the statement of a call of `jobs` in the current transaction of the database.

`toJobsTarget` of `producer/client.ts` treats a function as a client factory only if it has neither an `unsafe` nor a `query` method.
It recognizes a `Db` by `$database`, a database by `execute`, a postgres.js client by `unsafe`, and an executor by `query`, in that order, since a database also has a `query` method for templates.
It throws `TypeError` for a value of another kind when `createJobs` is called, or for a function that returns one when a call of `jobs` calls it.
The main entry declares the shapes of these clients, such as `JobsDatabase` and `UnsafeClient`, rather than importing them, since it depends on neither kern-db nor a client library.

### Executor

- The values are text or `null`, and the statement casts each one from text, so a client that serializes a parameter by its described type sends it as it is ([Drivers](drivers.md#statements)).
- The client of the application may rename the columns of a result, such as `postgres.camel`, and parse values: the statements on an executor read their rows through `fromExecutor` of `producer/client.ts` only by position, as `ValuesTarget`, which `queryOn` of kern-db, reading the columns by name, does not accept, and select only text.
- A row may be an object of values in the order of the columns or an array of values; `undefined` counts as `null`, and any other value but a string throws `TypeError`, since only a client that transforms text values gives one.
- The enqueue returns rows of one text column, a JSON array: one row `[id, created]` for one job or for each job that items fill, and a row `[id, key, created]` for each of many jobs, so that reading them depends neither on the name of the column nor on how the client parses JSON.
- The errors of the client reach the caller unchanged: an executor gives no `QueryError` or `ConnectionError`, while a database of kern-db gives its own.
- Producers must enqueue at `READ COMMITTED` ([PostgreSQL](postgresql.md#isolation)).

The user documentation connects these clients, which tests cover with commit and rollback:

- node-postgres: a `pg.Pool`, the `pg.PoolClient` of a transaction, or a function that returns either.
- postgres.js: `sql`, the client of a transaction, or a function that returns either.
- Orchid ORM: `db.$getAdapter()`, the adapter of the current transaction, though Orchid calls the adapter an implementation detail; its public `$query` takes a template or an SQL object whose values are named, `$name`, rather than positional parameters.
- kern-db and kern-orm: the `Database` or the `Db` itself.

## Scope

The jobs of a task defined with `scoped: true` take their [scope](README.md#terms) from the `scope` of `createJobs`, a string or a function that returns one; its value `ALL_SCOPES` serves [administration](administration.md#scope) only.
An enqueue of another task gives its job the empty scope ([decision](../decisions/enqueue/scope-of-jobs-api.md)):

- Whether a task is scoped is its setting, beside `lease` and `backoff`, since the jobs of a task either each belong to a scope, such as of a tenant, or serve every scope, and the jobs API gives only the value, so that one jobs API of an application serves both kinds.
- Each enqueue of a scoped task calls the function of the scope, such as one that reads the tenant of the current request; `enqueueMany` calls it once, also without payloads, when it calls no function of the client.
  An enqueue of another task neither calls the function of the scope nor checks the scope.
- `createJobs` throws `TypeError` for a scope other than a string, a function, `ALL_SCOPES`, `null`, or `undefined`, or a string that is not [storable text](README.md#terms), and `RangeError` for the empty string, which would enqueue into the empty scope, such as for a tenant whose id is empty, and for one longer than 128 bytes in UTF-8, the [limit of the column](data-model.md#columns).
- An enqueue of a scoped task rejects with `TypeError` without a scope or with `ALL_SCOPES`, since a job belongs to one scope, and with the errors of `createJobs` for a value that the function returns, before it calls the client.
- A handler gets the scope of its job as `job.scope`, and `null` for the empty scope ([Worker](worker.md#tasks)), so `createJobs` with `scope: job.scope` enqueues the scoped tasks into the scope of the job; the jobs of other tasks keep the empty scope.
- The type parameter `Scope` of a task, of its implementation, and of the context of its handler is `string` for a task of `scoped: true`, which an overload of `defineTask` selects for the literal `true`, and `string | null` for other tasks, whose jobs may keep the scope of an earlier definition that was scoped.

## Schema

The `schemaName` of `createJobs` names the PostgreSQL schema of the job table, `kern_jobs` by default, which [`migrateJobs`](migrations.md#migratejobs) creates.
`checkSchemaName` of `sql/job-table.ts`, which `createJobTable` and `migrateJobs` call, throws `TypeError` for a name that is not [storable text](README.md#terms) of 1 to 63 bytes in UTF-8.
PostgreSQL of a standard build truncates a longer name, also a quoted one, so two names that differ only after its 63rd byte would name one schema.

## Open questions

- Producers on postgres.js 3, whose `unsafe` has the same shape, are not tested.
