# Handlers decide terminal errors and retry delays by error classes

A handler fails its job for good by throwing `TerminalError`, and retries it after a given delay by throwing `RetryError`, whose delay replaces the backoff of the task:

```ts
export default chargeCardTask.implement(async ({ paymentId }) => {
  try {
    await stripe.paymentIntents.confirm(paymentId)
  } catch (error) {
    if (error instanceof Stripe.errors.StripeCardError)
      throw new TerminalError("card declined", { cause: error })
    if (error instanceof Stripe.errors.StripeRateLimitError)
      throw new RetryError("rate limited", { delay: 60, cause: error })
    throw error
  }
})
```

The handler meets the error, such as a status of a response or a result of parsing, so it decides there.

## Alternative

A classifier of thrown errors, `isTerminal(error)`, and a backoff of the attempt and the error, `backoff(attempt, error)`, decide in one place without a `try` in each handler, also for errors of libraries:

```ts
export const chargeCardTask = defineTask("payments.charge", {
  isTerminal: error => error instanceof Stripe.errors.StripeCardError,
  backoff: (attempt, error) =>
    error instanceof Stripe.errors.StripeRateLimitError ? 60 : 2 ** attempt,
}).withPayload<{ paymentId: string }>()
```

In the definition, which producers import, the classifier brings the error classes of the code of handlers into producers, such as a web server that only enqueues.
In the implementation, as an option of `implement`, it would keep them to the worker.
Sidekiq passes the exception to `sidekiq_retry_in`, which computes the delay of the retry.

`instanceof` does not tell an error of another copy of kern-jobs in the process, which the classes give up.
