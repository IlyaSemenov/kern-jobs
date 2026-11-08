# kern-jobs Agent Guide

## Overview

kern-jobs is a background job library on PostgreSQL for TypeScript.

Read the user documentation in [docs/](docs/) completely before changing the public API, package behavior, supported runtimes, or user documentation.

Before changing a module, read [architecture/README.md](architecture/README.md) and the files that its map lists for the module, and keep them in sync with the code.
Keep [architecture/](architecture/README.md) to the code as it is: record a contract in the file of its topic and unresolved design of the code in its Open questions; list a new file in the map.
Before writing a SQL statement of the core, also read [Statements](architecture/drivers.md#statements).
In architecture/, state what users must do as a requirement of the contract, such as "must run in autocommit", rather than as a description of what users do.

Record a decision in [decisions/](decisions/README.md) when an alternative has a real advantage that the chosen design gives up, such as in simplicity, speed, or a stronger guarantee, or looks better than it measures, so that it would be proposed again; keep a choice without such an alternative in architecture/ as its contract with a reason of one clause.
Whenever you learn how another job queue handles a scenario of kern-jobs, such as from its issues, documentation, or sources, or while researching a design, record it in [comparison/](comparison/README.md).
Track work not done yet in [backlog/](backlog/README.md).
Follow the rules of the index of each of them.

Read the relevant guide in [development/](development/README.md) before starting:

- [testing.md](development/testing.md) before writing, changing, or running tests or benchmarks;
- [user-documentation.md](development/user-documentation.md) before changing `docs/` or `README.md`;
- [supported-versions.md](development/supported-versions.md) before changing the supported versions of PostgreSQL, Node.js, Bun, or TypeScript.

Extend this guide only with stable, non-obvious conventions, architecture, contracts, workflows, and gotchas that most changes need; put those of one topic in the guide or the index of that topic.
Do not catalog files or restate information evident from their names and locations.

## Release status

The package is at version 0.0.0 and has no users; remove this section with the initial release.

- Judge a change only by the contract it produces.
  Do not compare behavior with previous revisions, call changes breaking, or add compatibility paths, migration notes, or deprecations.
- Do not add changesets: the pending initial release covers every change.
- Keep the history to the single commit of the initial release and amend it with every change: do not add commits, split a task into commits, or propose commit messages.
- Code, tests, and user documentation of a change may land in separate steps; they must agree by the initial release.
- Change the schema in its first migration, `0001_create_job`, rather than in a new one: no database has applied it.

## Scope

- Keep production code in `src/`, and place tests as [development/testing.md](development/testing.md) describes.
- Name a directory of one concept in the singular, such as `parser/`, and a directory of like items in the plural, such as `rules/`.
- Keep the entry points limited to explicit public exports, divided as [architecture/README.md](architecture/README.md#entry-points) describes.
- Export from an entry every type that the inferred types of its public values reference, so that the declarations of a consumer module can name them; the package test emits such declarations.
- Treat `package.json` exports and supported runtimes as public contracts.

## Design

- With every change, look for refactorings that reduce the complexity of the code it touches, such as duplicated logic or a concept spread over several modules.
  Make those that ease the change, and propose the others in the final report, listed apart from its fixes.
- Give a domain rule or contract a function of its own at its first use, such as how a cache key is built or which values an option accepts, so that the next use finds it; give it a module of its own only when several modules use it.
  Do not extract code that only looks alike.
- Before adding a helper, search for an existing one that does the same.
- Prefer a change of kern-db or kern-migrations, in the [kern-orm](https://github.com/IlyaSemenov/kern-orm) repository, to a wrapper, workaround, or boilerplate in kern-jobs that fills a gap of their general contract, such as a check of what a value of kern-db is or raw SQL for what a helper of kern-migrations lacks; keep logic specific to the queue in kern-jobs.
  Propose such a change to the user as soon as you see it, with the API it adds and the code of kern-jobs it removes; keep a local workaround only until the change is released.

## Refactoring

- When moving or splitting code, move its tests to follow it.

## Documentation

- Update the affected pages of `docs/` together with a change to the public API or behavior.
- Write public documentation and JSDoc text for package users who do not know the implementation.
- When a change makes an existing statement false, fix that statement rather than add an explanation next to it.
- Add JSDoc to every exported declaration and to internal helpers whose contract, inputs, output, or failure behavior is not obvious.
- Add inline comments beside every non-obvious invariant, algorithmic choice, safety constraint, and intentionally limited behavior.
- Update nearby JSDoc and inline comments whenever the documented code changes, and remove comments that no longer apply.
- Do not narrate self-evident syntax or restate what a name already communicates.
- Describe a default only when readers need it to make a decision or avoid surprising behavior.
- Use One Sentence Per Line for connected prose, including JSDoc and code comments: do not wrap a sentence across lines.
- Keep semantically connected explanations as prose paragraphs, and use lists for separate assertions.
- Link a file of another repository by a tag or a commit, such as of the version you checked, never by a branch such as `main`, whose file changes.

## Changesets

- Add changesets as [.changeset/README.md](.changeset/README.md) describes.

## Tests

- Tests need PostgreSQL: set `DATABASE_URL` in `.env.test.local`, copied from `.env.test.local.example`.
- Reproduce a defect with a deterministic test before fixing it, including one that the randomized test `src/worker/fuzz.test.ts` finds.

## Checks

- Run the checks on the database of `.env.test.local` only; CI runs the tests on the other supported versions of PostgreSQL.
- Run the `types` script when public types or TypeScript configuration change.
- Run the `test` script when behavior changes.
- When claiming, settlement, leases, or the worker change, also run `src/worker/fuzz.test.ts` with several values of `FUZZ_SEED`.
- Run the `test:package` script when package exports, declarations, the build configuration, or supported runtimes change; it builds the package first.
- Run the `docs:build` script when site pages or the VitePress configuration change.
