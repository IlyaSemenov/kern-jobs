# The scope of the jobs API limits retry and delete

With a `scope` of `createJobs`, retry and delete act only on the jobs of that scope, and call its function at each call; without one, they act on the jobs of every scope.
A jobs API of one scope, such as of the current request, then never changes a job of another scope, though a scope is no access control.

Ignoring the scope, so that retry and delete find a job by its id in any scope, lets the one jobs API of an application, whose scope function reads the current request, serve scripts of operators outside a request, and reach the jobs of tasks that are not scoped, which it enqueues too.
But a call through the jobs API of one scope would then change a job of another scope whose id it was given.
