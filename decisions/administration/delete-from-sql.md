# An application deletes jobs from SQL by a statement of its own

An application deletes pending and failed jobs where it has no `jobs`, such as in a migration of its own or from psql, by a `DELETE` on the job table with a condition on `state`, and filters them by any condition of SQL:

```sql
delete from kern_jobs.job
where state = 'pending' and task = 'newsletter.send' and payload @> '{"campaignId": 42}'
```

The table and the columns that it filters by, `id`, `state`, `task`, `scope`, `key`, `payload`, and `failed_at`, become a contract that binds later migrations of the table.

SQL functions of the schema that mirror `deletePending` and `deleteFailed` would leave the table and these columns free to change and would refuse an active row by themselves:

```sql
select kern_jobs.delete_pending(task => 'newsletter.send', payload_contains => '{"campaignId": 42}')
```

- They would duplicate the filters of the jobs API.
- They would need a spelling for an unscoped job and for every scope, which a condition on `scope` gives without one: `scope = ''`, or no condition.
- A filter of the payload by an argument, such as by containment, is narrower than a condition of SQL, which applies any operator of `jsonb` to a payload whose shape the application knows.

A filter of the payload in `deletePending` of `jobs` would have the same limits, and an application that needs one in JavaScript runs such a `DELETE` on its own client.

Graphile Worker gives its administration as SQL functions of its schema, such as `remove_job` ([source](https://github.com/graphile/worker/blob/v0.18.0/sql/000018.sql#L177-L206)).
