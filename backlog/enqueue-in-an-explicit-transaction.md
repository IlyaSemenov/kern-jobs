# Enqueue in an explicit transaction

`jobs.using(client).enqueue(…)` for an application that passes the client of a transaction explicitly rather than in an ambient context, which otherwise needs `createJobs` with that client.
