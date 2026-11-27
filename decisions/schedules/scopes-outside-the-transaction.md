# The function of the scopes runs outside the transactions of the check

Decide under the lock of the row whether an enqueue is due, end that transaction without the enqueue, call the function of the scopes, and run the check again as of the time of the first transaction, with the list.
While the function runs, the check holds neither a connection nor the lock of the row, and the cursor that the second transaction locks, waiting for a worker that holds the row, decides whether its list is still needed.
Several workers can therefore call the function for one occurrence, and all but one drop their lists; a check that enqueues takes two transactions, and the second fails at the lock timeout behind a worker that holds the row longer.

## Under the lock of the row

The transaction could call the function once it has locked the row and found an enqueue due.
Only the worker that holds the lock calls the function, and the check stays one transaction.
The function is code of the application: while it waits, such as for a registry over the network, the worker holds a connection and the lock, which the lock timeout of the transaction does not bound, and a function that reads through a pool that the transaction took the last connection of waits for itself.

## The second transaction decides by its own start

After the function, the transaction could check as any other, by the time of its own start, and use the list if an enqueue is due.
It needs no time passed from the first transaction, and its cursor reaches the current minute.
A function that returns after the end of the minute of an occurrence then loses it for a schedule without catch-up: a check at 03:00:50 whose function takes 15 seconds decides at 03:01:05, when only 03:01 is eligible.

## A reading of the cursor without the lock

One statement outside a transaction could read the time and the cursor, and the worker decide by them whether to call the function, before the only transaction of the check.
A check of a minute that the cursor covers then costs one read, without a transaction or a row lock, and a check that enqueues one transaction.
The reading decides without the lock, so the transaction can find an enqueue due that the reading did not, such as after another worker registers the schedule at an earlier minute, and needs the scopes after all, or loses an occurrence that the next check finds past its catch-up window.
The reading also waits outside the lock timeout of the transaction, unless it takes a transaction of its own.
