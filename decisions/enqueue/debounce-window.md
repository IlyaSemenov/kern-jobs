# A debounce window opens at its first debounce event, and its run time never moves backwards

Each debounce event of a window demands a due time by its own settings, and `run_at` is the greatest of these demands and `not_before`:

```text
due:     least(now + delay, window_started_at + maxWait)
run_at:  greatest(not_before, run_at, due)
```

A debounce enqueue over a pending row whose latest enqueue had another mode opens a new window at its own time, as the insert does.

So the order in which the events of a window commit does not change `run_at`, also when producers of different versions pass different settings during a deploy.
A smaller `delay` or `maxWait` never makes the job due earlier: after an event with delay 8 and maxWait 10 at 0, an event with maxWait 3 at 1 leaves `run_at` at 8.
A window that a mistaken setting pushed far, such as a delay in milliseconds read as seconds, keeps its run time after the fix, until an enqueue of the key with `keyMode: "replace"` resets it.
An enqueue in another mode ends the window, so debounce with delay 8 at 0, `keep` at 1, and debounce with delay 2 at 2 give `run_at` 4: the new window does not inherit the run time of the earlier one.

## The cap of the latest event

```text
due:     now + least(delay, maxWait)
run_at:  greatest(not_before, least(greatest(run_at, due), window_started_at + maxWait))
```

A smaller `maxWait` applies at once: the next event pulls the run time of an open window back to the new cap, also one that a mistaken setting pushed far.
But `run_at` then depends on the commit order of producers with different settings: events with delay 8 and maxWait 10 at 0, with maxWait 3 at 1, and with maxWait 10 at 2 give `run_at` 10 when the event at 2 commits last and 3 when the event at 1 does.
A smaller `delay`, in turn, never moves `run_at` back, so the two settings would apply differently.

## A window from the insert of the row in any mode

`maxWait` would bound the wait of the pending job from its first enqueue, whatever its mode.
But a debounce enqueue over a row that has been pending longer than `maxWait` makes it due at once and ignores its own `delay`: `replace` with delay 100 at 0 and debounce with delay 2 and maxWait 10 at 50 give `run_at` 10, so the first event of a burst can run before the remaining events arrive.
