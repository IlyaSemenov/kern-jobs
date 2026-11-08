# Fencing by a claim id

Each claim takes a new value of a sequence as its claim id, and every transition from `active` matches the id of the row and the claim id.

Matching the id, the worker, and `attempts` needs no extra column, but repeats after a return to pending: the reaper returns a job, a `replace` resets its attempts, and the same worker claims it again with the same worker and attempts.
A stale complete of the first claim would then delete the new execution, which a test reproduces.
