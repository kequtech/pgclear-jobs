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
make historical migrations import a live installer or the newest package. Tenon's `jobs setup`
creates this installation pair offline and recognizes an existing pair without rewriting it.

## Upgrade storage version 1 to 2

Jobs 0.2.1 uses storage version 2, adding nullable `schedules.request_key`. Existing jobs, attempts,
retry timing, claims, successors and ordinary schedules retain their state. New installations use
`jobsMigration`; existing installations use a **new** migration containing `jobsUpgrade.up`:

```ts
import { jobsUpgrade } from '@kequtech/pgclear-jobs/migrations';
// { from: 1, to: 2, up: string, down: string }
```

1. Stop producers and schedulers/workers using the old package. Await normal worker cleanup; do
   not reset running records or discard outstanding work.
2. Save/review the exported upgrade SQL as a new immutable application migration. Apply it in a
   transaction as the storage owner. It checks the source version and takes the installation lock.
3. Update all producers/workers to the new package, verify grants, then restart them. Old workers
   reject version 2, and new workers reject version 1. Mixed-version operation is unsupported.

Neither `migrate up` nor worker startup applies this upgrade. A failed upgrade transaction leaves
version 1 and its data intact. Repeated upgrade SQL rejects the wrong source version; the application's
migration history owns exactly-once application. The CLI has no automatic upgrade engine.

For rollback, stop producers/workers again and remove or convert every keyed schedule to an ordinary
schedule. `jobsUpgrade.down` refuses while request keys remain, then removes only the new column and
restores version 1. Shared jobs remain ordinary keyed requests; the old package can execute them.
Reversal does not undo jobs or business effects produced while version 2 was active.

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
