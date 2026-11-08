# Static options in defineTask, options of a payload in withOptions

`defineTask` takes the settings of a task and its static enqueue options in one object, and `withOptions` takes only a function that returns the options of a payload, which replace the static ones field by field:

```ts
export const indexProductTask = defineTask("search.indexProduct", { lease: 120, delay: 5 })
  .withPayload<{ productId: string }>()
  .withOptions(({ productId }) => ({ key: `product:${productId}` }))
```

The common task with static options needs no chain.
A function of the payload comes after `withPayload`, since TypeScript cannot take an explicit type argument of the payload and infer the literal type of the name in one call.

## Alternatives

One object in `defineTask` with the payload type given by a value removes `withPayload`, `withOptions`, and `TaskBuilder`, and the same field could later take a Standard Schema of the payload:

```ts
export const indexProductTask = defineTask("search.indexProduct", {
  payload: payloadOf<{ productId: string }>(),
  lease: 120,
  options: ({ productId }) => ({ key: `product:${productId}`, delay: 5 }),
})
```

TypeScript 7.0.2 and 6.0.3 infer the literal name and the payload from such an object in any order of its fields.
But the marker is a value without meaning at run time, and a task with only a payload type is longer than with `withPayload`.
Trigger.dev defines a task in one object, whose handler annotates its payload.

`withOptions` that also takes an object of static options gives two places to every static option, and a method for the settings anywhere in the chain needs several states of the builder in the declarations of consumers.
