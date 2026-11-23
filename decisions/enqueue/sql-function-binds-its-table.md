# The SQL function binds its table at its creation

The SQL function `enqueue` has a SQL-standard body, `BEGIN ATOMIC`, which PostgreSQL parses when the migration creates it, in the search path of the migration, and binds to the job table of the schema by its identity rather than by its name.
A call depends neither on the `search_path` of the caller, such as one with a temporary table `job` or a schema of a tenant first, nor on the name of the schema, which a rename may change, such as of a schema of a tenant that a dump restores under another name.

Such a body has no `RAISE`, so a second function of the schema, `check_enqueue` in PL/pgSQL, checks the arguments and raises the errors; it names no object of the schema, and its fixed search path, `pg_catalog, pg_temp`, gives its comparisons the operators of `pg_catalog` whatever the search path of the caller and the name of the schema.

```sql
create function enqueue(task text, …, out id bigint, out created boolean) language sql
begin atomic
  select check_enqueue(enqueue.task, …);
  insert into job as j (…) select … on conflict … do update set … returning j.id, j.xmax = 0;
end
```

## A PL/pgSQL function with a fixed search path

One PL/pgSQL function with `SET search_path FROM CURRENT` would check its arguments itself and find the table by the search path of the migration, which the clause stores.

The clause stores the name of the schema, so a rename of the schema leaves the function searching a schema that no longer exists.

## Qualified names written by the migration

The migration could write the name of its schema into the body of a PL/pgSQL function, such as by `EXECUTE format('… %I.job …', current_schema())`.
The body then names the schema as text too, so a rename breaks it in the same way, and the migration builds its SQL at run time rather than declaring it.
