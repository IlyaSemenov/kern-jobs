# Enqueueing Jobs

Enqueue a job of a [task](./) with its payload:

```ts
const { id } = await jobs.enqueue(sendEmailTask, { to: "ann@example.com", subject: "Welcome" })
```

It resolves to `{ id, created }`: the id of the job, and whether the job was created rather than merged into the pending job of its [key](./keys).

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
To [override options](#overriding-options), pass them as the third argument; they apply to every payload, and options that differ by payload, such as a key, come from `withOptions` of the task:

```ts
await jobs.enqueueMany(indexProductTask, payloads, { delay: 0 })
```

It resolves to an array of `{ id, created }`, one for each payload in their order.

- Pass each key at most once: two payloads that get the same key reject the call.
- The new jobs get ids in the order of the payloads, so a [queue](./queues) starts those that are due at the same time in that order.
- `enqueueMany` locks the pending jobs of the keys in the order of the payloads, so two transactions that enqueue the same keys in different orders can deadlock.
  PostgreSQL then aborts one of them with a deadlock error, `40P01`: retry its whole transaction.
