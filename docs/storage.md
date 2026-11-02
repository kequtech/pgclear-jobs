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

## Change existing storage

Jobs 0.3.0 requires storage version 3. The package provides one canonical jobsMigration.up/down pair
for the current schema. It does not provide version-specific upgrade or downgrade SQL.

When upgrading an application, compare its saved Jobs schema with the current canonical SQL and
author the necessary changes as a new application migration. Preserve queued work and immutable
migration history. Stop old producers/workers, apply the reviewed migration as the storage owner,
verify grants and restart matching packages. Test it against representative retained jobs, attempts
and schedules. Do not apply the fresh-install down/up pair to upgrade populated storage.

Version 2 → 3 adds jobs.cancel_requested_at and permits cancelled attempt outcomes. Version 1 → 2
added schedules.request_key. The canonical SQL defines their exact types and constraints. Choose
reversal behavior deliberately: active cancellation intent and cancelled attempt history must not
be silently erased when reverting to an older schema.

Neither setup, migrate up nor worker startup upgrades existing storage. Repeated installation only
validates compatibility; it never resets an incompatible schema. If an application script imports
jobsMigration directly, declare pgclear-jobs as a direct dependency under strict package managers.

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
