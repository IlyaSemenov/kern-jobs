# The key of a payload comes from `withKey`

`withKey` takes a function that returns the key of a payload of a task, or `null` for none, or the key of an item of a batch task, and the options of `withOptions` give no key:

```ts
export const indexProductTask = defineTask("search.indexProduct", { delay: 5 })
  .withPayload<{ productId: string }>()
  .withKey(({ productId }) => `product:${productId}`)

export const publishPostsTask = defineTask("posts.publish", {
  keyMode: "debounce",
  delay: 2,
  maxWait: 10,
})
  .withBatchPayload<{ siteId: string; postId: number }>({ maxItems: 100 })
  .withKey(post => `site:${post.siteId}`)
```

The key chooses the pending job that an enqueue merges into, and the other options describe that job.
So the two kinds of tasks differ only in that a batch task has no `withOptions` and its overrides give only the key: another option of an enqueue would apply to the whole batch, which holds the items of other enqueues too.
The items of an enqueue that calls `withKey` must have one key, as the payloads of one `enqueueMany` that have keys must have distinct ones.
A key of the overrides replaces that of `withKey`, which is then not called, as it replaces the key of `defineTask`.

## The key in `withOptions`

One function of the payload could return the key with the other options, in one place.
A batch task would then need a function of an item under another name, for a function of the same shape, or a task would have two ways to compute its key and a rule of precedence between them.

## `withItemKey` of batch tasks only

A function of the key of an item on batch tasks alone would leave the API of the other tasks as it is.
But it names one concept twice, and the key of the other tasks stays in `withOptions` beside the options that describe the job.

## No function of the key of a batch

Without a function of the items, every caller passes a key that differs between enqueues, such as of the site of its posts, and the rule that relates the key to the items lives at each call site, where a key and the items can disagree, so that an array of two sites appends to the batch of one.
It needs neither `withKey` on batch tasks nor the check that the keys of the items agree.

## `withKey` rejecting the key of an enqueue

A task with `withKey` could take no key of the overrides, so that one rule gives the key of each enqueue.
It needs a type of each kind of a task with `withKey` and overloads of the enqueues and of `defineSchedule` without the key, and a variable typed as a `Task` or a `BatchTask` loses the difference, so that a check at run time rejects what the types accept.

## `withOptions` of the array of an enqueue

A function of the whole array of an enqueue of a batch task could give the key and every other option in one place.
But it gives one key for items that may belong to several, such as `items[0].siteId`, and an array of two sites would append silently to the batch of the first, while its other options, such as a queue or a delay, would replace those of the whole batch.
