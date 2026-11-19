# Administration of scopes

Listing the jobs of a scope by task and state without their payloads, which may hold personal data; `deletePending` and `deleteFailed` delete its pending and failed jobs, such as when a tenant is deleted.

Preliminary, the end of a scope, such as of a deleted tenant, is a protocol of the application, which kern-jobs supports but cannot run alone:

1. The application marks the scope as ending, and stops enqueuing its jobs, also in producer transactions that are still open; kern-jobs has no pause of a scope, and its workers run the jobs of every scope.
2. A handler checks the mark before its side effects, and throws `TerminalError` for a scope that has ended, so that its job does not return for more attempts; `TerminalError` fits a scope known to have ended, not any failure to reach its data.
3. The application deletes the pending jobs of the scope with `deletePending`, and the failed ones with `deleteFailed`.
4. It cannot stop the running jobs of the scope: each completes, fails for good, or returns to pending for another attempt, so it repeats step 3 until the scope has no job left.
   No job of the scope in the table does not prove that no handler of it runs: a handler that ignores its abort runs on after its job returned or was deleted, so the application decides when the data of the scope can go, such as after the longest run of a handler.

Open:

- Whether workers skip the jobs of a scope that the application pauses, so that step 4 ends sooner.
