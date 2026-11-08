# Comparison with other projects

How other job queues handle the scenarios of kern-jobs, by topic, as checked in their documentation, issues, sources, or a named version.
Each scenario states what kern-jobs does, and gives each project a verdict:

- `fails`: it gets wrong what kern-jobs covers.
- `lacks`: it lacks what kern-jobs has.
- `has`: it has what kern-jobs lacks.
- `differs`: it does the task another way.
- `same`: it does what kern-jobs does.

## Projects

Records of other projects, here and in [decisions](../decisions/README.md), describe these versions unless a record names another one, and a record of an issue or a pull request tells what the project did then.

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
- Schedules in cron expressions across several workers.

## Writing a record

- Record a scenario in the file of its topic, under a heading of the scenario.
  - Open the scenario with a sentence of what kern-jobs does.
  - Give each project an item with its name, a link to its issue, documentation, or source, and a verdict.
  - State what the project does in the scenario, such as which jobs run after which events, rather than a judgment such as a loss of jobs.
- Name another project, here and in decisions/, as [Projects](#projects) lists it, without a link to the project and without a version unless it differs from the listed one; write a record of an issue or a pull request in the past tense.
  List a new project there with its link and the version you checked.
- Do not link comparison/ back to architecture/.
- Also record a scenario that another project fails while kern-jobs covers it in a comment of the test that checks the scenario, such as `// Graphile Worker ran jobs of one queue concurrently: <issue URL>`.
- Keep a capability that another project has and kern-jobs lacks, such as `key_strict_fifo` of pg-boss, in the item of [backlog/](../backlog/README.md) that would add it, and move it here when the item is deleted.
- Add a scenario worth comparing that the topics do not cover yet to [Scenarios to compare](#scenarios-to-compare).
