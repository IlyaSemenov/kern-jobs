# A later signal forces the stop only once 2 seconds have passed since the first

`waitForSignalAndStop` and `stopOnSignals` begin the stop at the first SIGTERM or SIGINT, ignore another of them within 2 seconds of it, and force the stop at a later one while the worker is still stopping.
Their listeners stay for those 2 seconds also after a fast stop, while the application cleans up, and are removed once the stop has ended and the window has passed, so that the next signal gets its default action.

## Alternatives

Forcing the stop at any second signal lets an operator end a stuck stop at once with a second Ctrl+C.
But one Ctrl+C can reach the process twice: the terminal sends SIGINT to the whole process group, and a parent runner, such as a script runner or a process manager, forwards it to its child; an orchestrator and a wrapper may also each send a signal.
Such a pair would force the stop, so that a handler that ends on the abort is not settled and its job spends its attempt, failing for good on its last attempt.

Forcing only at a repeat of the same signal, and letting the other one join, keeps the immediate reaction to a deliberate repeat and still joins an orchestrator's SIGTERM with a wrapper's SIGINT.
But the doubled Ctrl+C is two SIGINTs, so it still forces the stop.

Removing the listeners once the stop has ended, rather than at the end of the window, lets a signal during the cleanup of the application end the process at once.
But the second copy of the Ctrl+C that began a fast stop then kills the process in the middle of the cleanup, before the application has closed its clients or sent its error reports.

Graphile Worker ignores further termination signals for 5 seconds and starts a forceful shutdown at a later one.

The window gives up the immediate reaction to a deliberate second signal: one within 2 seconds of the first is ignored, and an operator who wants to force the stop sends another after them.
