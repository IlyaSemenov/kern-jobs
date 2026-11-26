# Enqueueing Jobs

Enqueue a job of a [task](./) with its payload:

```ts
const { id } = await jobs.enqueue(sendEmailTask, { to: "ann@example.com", subject: "Welcome" })
```

It resolves to `{ id, created }`: the id of the job, and whether the job was created rather than merged into the pending job of its [key](./keys).
To enqueue the items of a [batch task](./keys#batches), which can fill several jobs, use `jobs.enqueueItems`, which resolves to such an object for each job.

An enqueue joins the transaction of the database client of `jobs`, so enqueue a job in the transaction that writes its data: workers see the enqueue when the transaction commits, and a rollback undoes it.
[Database Clients and Transactions](../database-clients) shows how to enqueue in the transactions of each client.

## Delays

To run a job later, pass `delay` in seconds, or the time as `runAt`, in the [options of its task](./#options-of-a-task) or in the [options of an enqueue](#overriding-options):

```ts
export const checkPaymentTask = defineTask("payments.check", { delay: 60 }).withPayload<{
  paymentId: string
}>()

await jobs.enqueue(sendEmailTask, { to, subject }, { runAt: sendAt })
```

A job with a `runAt` in the past is due at once.

## Overriding options

To override the [options of its task](./#options-of-a-task) for one job, pass them as the third argument of `jobs.enqueue`.
They replace the options of the task one by one:

```ts
await jobs.enqueue(indexProductTask, { productId }, { delay: 0 })
```

- An option passed as `undefined` keeps the option of the task.
- `key: null` and `queue: null` enqueue the job without the key or queue of the task.
- `delay` and `runAt` replace each other: passing one drops the other of the task.

## Enqueueing many jobs

To enqueue a job of a task for each of many payloads, such as to index the products of an import, pass the payloads to `jobs.enqueueMany`:

```ts
await jobs.enqueueMany(
  indexProductTask,
  productIds.map(productId => ({ productId })),
)
```

It enqueues each payload as `jobs.enqueue` does, in one statement: either all of the jobs or none.
To [override options](#overriding-options), pass them as the third argument; they apply to every payload, and a key that differs by payload comes from `withKey` of the task, and other such options from `withOptions`:

```ts
await jobs.enqueueMany(indexProductTask, payloads, { delay: 0 })
```

It resolves to an array of `{ id, created }`, one for each payload in their order.

- Pass each key at most once: two payloads that get the same key reject the call.
- The new jobs get ids in the order of the payloads, so a [queue](./queues) starts those that are due at the same time in that order.
- `enqueueMany` locks the pending jobs of the keys in the order of the payloads, so two transactions that enqueue the same keys in different orders can deadlock.
  PostgreSQL then aborts one of them with a deadlock error, `40P01`: retry its whole transaction.

## Enqueueing from SQL

To enqueue jobs from SQL, such as in a migration of your application that indexes the existing products, call the function `enqueue` in the schema of kern-jobs, `kern_jobs` by default:

```sql
select kern_jobs.enqueue(
  task => 'search.indexProduct',
  payload => jsonb_build_object('productId', p.id::text),
  key => 'product:' || p.id,
  delay => 5
)
from product p
where p.active;
```

[`migrateJobs`](../getting-started#create-the-schema) creates the function, so run it before the migrations of your application that call it.

It enqueues a job as `jobs.enqueue` does, in the transaction of the statement, and takes the options under these names: `payload`, `scope`, `key`, `queue`, `key_mode`, `run_at`, `delay` and `max_wait` in seconds, `max_attempts`, and `reset_attempts`.

- It does not know your task definitions: pass the name of the task and every option that the task would give, such as the key of its `withKey`, and a payload of the type of the task, which nothing checks.
  For a task with `key: true`, pass `key => ''`.
- For a [scoped](./scopes) task, pass the scope of the job as `scope`, which must not be empty: a worker fails a job of a scoped task without a scope without running its handler.
  For another task, omit `scope`, which makes the job unscoped, as `''` does.
- An option passed as NULL is the same as an omitted one, so a column that is NULL omits its option, such as the key; `task` and `scope` reject NULL.
- An omitted payload reaches the handler as `undefined`; pass `'null'::jsonb` for `null`.
- The rows of one key merge into its pending job one after another, as separate enqueues do.

It returns a composite value with two fields: the `id` of the job and whether it was `created`.
To read them as columns, call it in `FROM` with arguments that take values of the row:

```sql
select p.id as product_id, e.id as job_id, e.created
from product p
cross join lateral kern_jobs.enqueue(
  task => 'search.indexProduct',
  payload => jsonb_build_object('productId', p.id::text),
  key => 'product:' || p.id,
  delay => 5
) e
where p.active;
```

PostgreSQL may call a function in `FROM` whose arguments take no value of the row only once for all rows, so call such a function in the select list to enqueue a job for each row.

Do not expand it as `(kern_jobs.enqueue(…)).*`, which PostgreSQL evaluates once for each column, enqueueing each job twice.

### Items of a batch task

To enqueue the items of a [batch task](./keys#batches) from SQL, call `kern_jobs.enqueue_items` with the name of the task, a JSON array of items as `items`, its `maxItems` as `max_items`, and the key as `key`, which a non-empty array requires:

```sql
select e.id, e.created
from kern_jobs.enqueue_items(
  task => 'posts.publish',
  items => jsonb_build_array(1, 2, 3),
  max_items => 100,
  key => ''
) e;
```

It appends the items as `jobs.enqueueItems` does and takes the other options of `enqueue`, except `payload`; it rejects NULL as `items` and `max_items`, and `keep` as `key_mode`.
It returns a row for each job that took items, in the order of the items: the `id` of the job and whether it was `created`; an empty array returns none.

### Privileges

The SQL functions run with the privileges of the role that calls them.
Grant that role:

- `USAGE` on the schema `kern_jobs`;
- `SELECT`, `INSERT`, and `UPDATE` on the table `job`;
- `EXECUTE` on the functions `enqueue`, `enqueue_items`, `check_enqueue`, `check_items`, and `check_merge`.
