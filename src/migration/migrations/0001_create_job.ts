// First migration of kern-jobs: the job table, its claim sequence, and its indexes.
import { createIndex, createSequence, createTable, defineMigration } from "kern-migrations"

export default defineMigration([
  createSequence("claim_seq"),
  createTable(
    "job",
    {
      id: "bigint generated always as identity primary key",
      // The limits in bytes keep the entries of the indexes on (scope, task, key) and (scope, queue, run_at, id) within the 2704 bytes of a B-tree entry also for names that do not compress.
      task: "text not null check (octet_length(task) <= 128)",
      scope: "text not null default '' check (octet_length(scope) <= 128)",
      queue: "text check (octet_length(queue) <= 1024)",
      key: "text check (octet_length(key) <= 1024)",
      key_mode:
        "text not null default 'replace' check (key_mode in ('replace', 'keep', 'debounce'))",
      payload: "jsonb",
      run_at: "timestamptz not null",
      not_before: "timestamptz",
      window_started_at: "timestamptz not null",
      created_at: "timestamptz not null",
      attempts: "int not null default 0 check (attempts >= 0)",
      max_attempts: "int not null check (max_attempts >= 1)",
      state: "text not null default 'pending' check (state in ('pending', 'active', 'failed'))",
      claim_id: "bigint",
      locked_by: "text",
      locked_at: "timestamptz",
      lease_until: "timestamptz",
      last_error: "text",
      failed_at: "timestamptz",
    },
    {
      constraints: {
        job_active_check:
          "check ((state = 'active') = (claim_id is not null) and (state = 'active') = (lease_until is not null))",
        job_failed_check: "check ((state = 'failed') = (failed_at is not null))",
        // A run time never precedes the end of the backoff; a row without a backoff, whose not_before is null, passes.
        job_not_before_check: "check (run_at >= not_before)",
      },
    },
  ),
  // Claiming jobs without a queue.
  createIndex("job", ["run_at"], {
    name: "job_plain_ready_idx",
    where: "state = 'pending' and queue is null",
  }),
  // The queue scan and the queue head.
  createIndex("job", ["scope", "queue", "run_at", "id"], {
    name: "job_queue_ready_idx",
    where: "state = 'pending' and queue is not null",
  }),
  // At most one pending job per (scope, task, key).
  createIndex("job", ["scope", "task", "key"], {
    name: "job_pending_key_idx",
    unique: true,
    where: "state = 'pending' and key is not null",
  }),
  // Queue mutex: at most one active job per (scope, queue).
  createIndex("job", ["scope", "queue"], {
    name: "job_active_queue_idx",
    unique: true,
    where: "state = 'active' and queue is not null",
  }),
  // The reaper.
  createIndex("job", ["lease_until"], {
    name: "job_active_lease_idx",
    where: "state = 'active'",
  }),
])
