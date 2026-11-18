# Comparison with other projects

How other job queues handle the scenarios of kern-jobs, by topic, as checked in their documentation, issues, sources, or a named version.
Each scenario states what kern-jobs does, and gives each project a verdict:

- `fails`: it gets wrong what kern-jobs covers.
- `lacks`: it lacks what kern-jobs has.
- `has`: it has what kern-jobs lacks.
- `differs`: it does the task another way.
- `same`: it does what kern-jobs does.

## Projects

Use the version listed here for every comparison of a project, including [decisions](../decisions/README.md) and the [user guide](../docs/comparison.md).
When changing a project's version, recheck all its comparison items against the new version and update their source links together.

- [Graphile Worker](https://github.com/graphile/worker) 0.18.0.
- [graphile-worker-tasks](https://github.com/IlyaSemenov/graphile-worker-tasks) 2.6.1, typed task definitions over Graphile Worker.
- [graphile-worker-orchid](https://github.com/IlyaSemenov/graphile-worker-orchid) 2.5.1, the utilities of Graphile Worker over Orchid ORM.
- [pg-boss](https://github.com/timgit/pg-boss) 12.38.1.
- [BullMQ](https://github.com/taskforcesh/bullmq) 6.7.5.
- [Inngest](https://github.com/inngest/inngest-js) 4.26.2, its SDK for TypeScript.
- [Sidekiq](https://github.com/sidekiq/sidekiq) 8.1.7.

## Topics

- [Tasks](tasks.md)
- [Keys](keys.md)
- [Queues](queues.md)
- [Schedules](schedules.md)
- [Leases](leases.md)
- [Stopping](stopping.md)
- [Database clients](database-clients.md)
- [Migrations](migrations.md)
- [Administration](administration.md)

## Scenarios to compare

Scenarios worth comparing that the topics do not cover yet:

- Throttling: at most one job of a key per period, as `preserve_run_at` of Graphile Worker and `sendThrottled` of pg-boss.
- At most one running job per key without a queue, as the `singleton` policy of pg-boss.
- The delay from an enqueue to the start of its job, with polling and with LISTEN/NOTIFY.
- Claiming from many queues at once under load.

## Writing a record

- Record a scenario in the file of its topic, under a heading of the scenario.
  - Open the scenario with a sentence of what kern-jobs does.
  - Give each project an item with its name, a link to its issue, documentation, or source, and a verdict.
  - State what the project does in the scenario, such as which jobs run after which events, rather than a judgment such as a loss of jobs.
  - Cite an issue or a pull request only for behavior of the listed version, such as a missing feature, never for a defect that the listed version fixes.
- Use the project name from [Projects](#projects) as plain text in each item and give the version once in the project list.
  Describe historical issues and pull requests in the past tense.
  List a new project there with its link and the version you checked.
- Do not link comparison/ back to architecture/.
- Also record a scenario that another project fails while kern-jobs covers it in a comment of the test that checks the scenario, such as `// Graphile Worker ran jobs of one queue concurrently: <issue URL>`.
- Keep a capability that another project has and kern-jobs lacks, such as `key_strict_fifo` of pg-boss, in the item of [backlog/](../backlog/README.md) that would add it, and move it here when the item is deleted.
- Update [docs/comparison.md](../docs/comparison.md), a summary of comparison/ for choosing a queue, when a record that it summarizes changes; it gives the reviewed versions and review date and a link to the evidence of each claim about another project.
- Add a scenario worth comparing that the topics do not cover yet to [Scenarios to compare](#scenarios-to-compare).
