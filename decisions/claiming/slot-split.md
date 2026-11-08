# Slots split between jobs with and without a queue

A claim cycle gives half of the free slots to jobs without a queue and half to queued jobs, alternates the larger half between cycles, and gives the share that one class cannot use to the other.
Queues are visited round-robin from the cursor of the worker.

Claiming one class first and the other with the remaining slots needs one statement less per cycle, but a permanent backlog of the first class left the second without claims.
Visiting queues from the first one each cycle left late queues without claims while early queues had a backlog.

With three worker processes of two slots under an inflow above their capacity for five seconds, jobs without a queue and queued jobs got 543 and 545 claims, and each of 20 queues got 26–29.
