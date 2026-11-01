# Schema setup

Set `DATABASE_URL` in the environment or `.env`, then run:

```sh
npx pgclear-jobs migrate
```

For an application npm script, use `"jobs:migrate": "pgclear-jobs migrate"` and run
`npm run jobs:migrate`. Within this library's checkout, that script builds the command first.

There is one schema definition. Repeated installation preserves existing jobs. Installation is
atomic and serialized by an advisory transaction lock; neither producers nor workers apply DDL.
Workers check a small internal compatibility marker before starting.

To intentionally remove all Jobs storage:

```sh
npx pgclear-jobs migrate down
```

Down executes `DROP SCHEMA IF EXISTS pgclear_jobs CASCADE`, removing jobs, attempts, schedules and
objects depending on that namespace. Stop producers and workers first. A severe breaking schema
change can be handled with this explicit down/up reset; queued jobs and history will be lost.
There are no migration ranges, intermediate versions, or automatic destructive upgrades.

Programmatic setup is also available:

```ts
import { migrateJobs } from '@kequtech/pgclear-jobs';

await migrateJobs(pool);
await migrateJobs(pool, 'down');
```

`installJobs(pool)` remains a convenience for programmatic installation.

## Application migration files

Tenon ORM and other migration tools can extract the same SQL without opening a connection:

```ts
import { jobsMigration } from '@kequtech/pgclear-jobs/migrations';

// Write jobsMigration.up and jobsMigration.down into the application's migration files.
```

Both strings use the reserved `pgclear_jobs` namespace and include the same advisory lock as the
command. They contain no outer `BEGIN` or `COMMIT`; the application's migration runner owns that
transaction. Commit the concrete SQL files so later package updates cannot change historical
migrations. Tenon needs to copy and apply this pair, without understanding the Jobs tables or SQL.

Changing configuration never removes storage. Existing incompatible installations are rejected by
the up command and workers, leaving the developer to choose when to reset. The old `tenon_jobs`
prototype namespace is left alone. After an uncertain commit, inspect database state before retrying.
