# Testing helpers

Reading the pending jobs of a test in its transaction, and running them inside it, so that their handlers see its data.
Such helpers would give the semantics of the job table that the in-memory double of `kern-jobs/testing` lacks, such as an enqueue in a transaction that rolls back, merges by key mode, and debounce.
