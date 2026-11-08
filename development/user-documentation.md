# User documentation

How to write the user documentation of kern-jobs, the VitePress site in `docs/`, published to GitHub Pages.

## Site

- Keep `README.md` short and link to the site.
- Add every new page to the sidebar in `docs/.vitepress/config.ts`.

## Content

- Write user documentation as a guide to using the API in plain terms: what to call and what it does, not how it is typed or implemented.
- Start each section from what users want to achieve in their application, such as retrying failed requests, rather than from a description of the package or its concepts.
  Name only a task that users really have; when none fits, state plainly what the API does instead of inventing a task for a "To …" opening.
  Open with a short statement of the common case and its example, and put details, alternatives, and edge cases after the example.
- Other things being equal, write the happy path as an instruction of what to do, such as "Pass the options to `connect`", rather than as a description of the API, such as "`connect` takes options".
  This is a preference, not a rule: keep a description wherever it reads more clearly.
- Do not restate in prose what users see in their editor's types or learn from a TypeScript or runtime error.
- Document every guarantee that the application relies on, such as of delivery, retries, merges of keyed jobs, leases, transactions, and stopping.
  Mention another edge case only when users must write different code because of it; record other edge cases and invariants in [architecture/](../architecture/README.md) and cover them with tests.
- Write a rule that users must follow as an instruction, such as to call `close` before the process exits, rather than as the check or error that enforces it.

## Examples

- Use postgres.js and its driver, `postgresJsDriver` of `kern-jobs/db/postgres-js`, in the examples that need a database client, and show node-postgres only as an alternative, such as in [docs/database-clients.md](../docs/database-clients.md).
- Name the definition of a task with the suffix `Task`, such as `indexProductTask`, which leaves `indexProduct` to the function of the application that its handler calls.
- In the examples of an application, keep the definitions of tasks in `src/tasks.ts` and the code of the worker in `src/worker/`: each implementation of a task as the default export of a module of its own, such as `src/worker/tasks/index-product.ts`, the manifest that gathers them in `src/worker/manifest.ts`, and the start of the worker in `src/worker/start.ts`.
  A short example that runs on its own may keep all of it in one module.
