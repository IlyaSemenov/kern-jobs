// The column `failure_max_attempts`: the attempt budget of a failure job, which a claim stamps on its job when the implementation has `onFailure`, so that a terminal failure of the claim inserts a failure job.

import { addColumn, addConstraint, defineMigration } from "kern-migrations"

export default defineMigration([
  addColumn("job", "failure_max_attempts", "int"),
  // NOT VALID skips the scan of the existing rows under the lock of the table, which all pass, since their column is null; every later write is checked.
  // The name of the failure task, the name of the task with `:failure` after it, must fit the limit of `task`, so that the insert of a failure job cannot fail by it.
  addConstraint(
    "job",
    "job_failure_max_attempts_check",
    `check (failure_max_attempts is null
       or (failure_max_attempts >= 1 and octet_length(task) <= 120)) not valid`,
  ),
])
