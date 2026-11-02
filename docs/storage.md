# Storage setup, upgrades and grants

Jobs owns one fixed `pgclear_jobs` schema per database. Imports and worker startup never install
or upgrade it. Applications sharing a database must agree on storage ownership; job names are not
an authorization boundary.

## Install new storage

```sh
npx pgclear-jobs migrate
```

The command reads `DATABASE_URL` from the environment or `.env`. Explicit `up` is also accepted;
`--help` needs no credentials. `migrateJobs(pool)` and `installJobs(pool)` are programmatic equivalents.
Installation uses one transaction and an advisory lock. Repeating it validates the current version
and preserves data; an incompatible namespace rejects instead of being reset.

With an existing migration runner, save the concrete strings instead:

```ts
import { jobsMigration } from '@kequtech/pgclear-jobs/migrations';
// Write jobsMigration.up/down into a new application migration pair.
```

These contain no outer BEGIN/COMMIT; the runner supplies the transaction. Commit the SQL and never
make historical migrations import a live installer or the newest package. A query-library CLI may copy
this pair into its migration directory; ownership of saved SQL remains with the application.

## Upgrade storage

Jobs 0.3.0 requires storage version 3. Upgrades are concrete SQL, saved as new application migrations:

| Existing storage     | Export from @kequtech/pgclear-jobs/migrations | Result                                                |
| -------------------- | --------------------------------------------- | ----------------------------------------------------- |
| 1 (Jobs 0.2.0)       | jobsUpgrade                                   | 2: keyed schedules                                    |
| 2 (Jobs 0.2.1/0.2.2) | jobsCancellationUpgrade                       | 3: durable cancellation and cancelled attempt history |

For version 1, apply both pairs in order. Fresh installations use jobsMigration directly.
An application script importing these exports must declare pgclear-jobs directly; a transitive
installation through an ORM does not grant direct imports under strict package managers.

Choose the next unused migration number and save the SQL. For version 2, for example:

```ts
import { writeFile } from 'node:fs/promises';
import { jobsCancellationUpgrade } from '@kequtech/pgclear-jobs/migrations';

await writeFile('migrations/0005_jobs_cancellation.up.sql', jobsCancellationUpgrade.up, {
  flag: 'wx',
});
await writeFile('migrations/0005_jobs_cancellation.down.sql', jobsCancellationUpgrade.down, {
  flag: 'wx',
});
```

1. Stop producers and schedulers/workers using the old package; await normal cleanup.
2. Review/commit the new pair. Apply up in a transaction as the storage owner through the existing
   migration runner. The SQL checks its source version and takes the installation lock.
3. Verify grants and restart matching producers/workers. Mixed storage/package versions reject.

Neither setup, migrate up nor worker startup upgrades existing storage. A failed migration rolls
back its DDL and data changes. Applied application migrations remain immutable. The package has no
upgrade command; copying reviewed SQL is the upgrade workflow.

Before reversing version 3, stop producers/workers, finish outstanding cancellation requests and
remove cancelled attempt history using normal terminal-record removal/pruning. The down SQL refuses
to erase active intent or incompatible attempt outcomes. Version 2 → 1 also refuses while keyed
schedules remain; remove or convert them first. Reverse pairs in reverse order. Existing work and
business effects are not undone by reverting storage.

## Runtime grants

Use a storage owner for DDL and a separate runtime role. The shipped
[runtime grants example](../examples/runtime-grants.sql) assumes `jobs_runtime` already exists:

```sql
GRANT USAGE ON SCHEMA pgclear_jobs TO jobs_runtime;
GRANT SELECT ON pgclear_jobs.schema_version TO jobs_runtime;
GRANT SELECT, INSERT, UPDATE, DELETE ON pgclear_jobs.jobs, pgclear_jobs.attempts,
  pgclear_jobs.schedules TO jobs_runtime;
REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA pgclear_jobs FROM PUBLIC;
GRANT EXECUTE ON FUNCTION pgclear_jobs.request_job(uuid, text, jsonb, text,
  timestamptz, double precision, jsonb, boolean) TO jobs_runtime;
```

Triggers execute without a direct EXECUTE grant on their functions. The runtime role does not need
schema CREATE, ownership, or DDL rights. This recipe covers submissions, scheduling, execution,
retry, inspection and pruning; it is not per-definition isolation. Recheck grants after storage DDL.
A producer-only role can omit attempts privileges and DELETE, retaining jobs SELECT/INSERT/UPDATE,
request-function execution, and schedule privileges only if it registers schedules. The application
still owns database login, membership and business-table grants.

## Remove storage intentionally

```sh
npx pgclear-jobs migrate down
```

Stop producers/workers first. Down runs `DROP SCHEMA IF EXISTS pgclear_jobs CASCADE`, removing jobs,
attempts, schedules, history and dependent objects. It is not an upgrade procedure. Programmatically,
use `migrateJobs(pool, 'down')`. Repeated removal is safe; data loss is intentional.

A lost commit response does not prove rollback. Inspect authoritative state before retrying any
migration or compensating external effects.
