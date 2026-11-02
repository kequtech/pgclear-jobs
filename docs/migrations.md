# Schema setup

After installing `@kequtech/pgclear-jobs` and its pgclear peer, set `DATABASE_URL` in the environment
or `.env` and run:

```sh
npx pgclear-jobs migrate
```

For an application npm script, use `"jobs:migrate": "pgclear-jobs migrate"`, then
`npm run jobs:migrate`. Within this library's checkout the script builds the command first.
The command also accepts explicit `up`. `pgclear-jobs --help` needs no database.

There is one schema definition. Repeating installation checks compatibility and preserves existing
work. Installation is atomic and serialized by an advisory transaction lock. Neither importing the
package nor running a worker installs storage.

## Remove storage

Stop producers and workers, then intentionally remove the namespace:

```sh
npx pgclear-jobs migrate down
```

Down executes `DROP SCHEMA IF EXISTS pgclear_jobs CASCADE`. It removes jobs, attempts, schedules,
history, and objects depending on them. Repeated down is safe. A severe breaking schema change may
use this explicit down/up reset, with loss of queued jobs accepted by the developer. There are no
intermediate versions or automatic destructive upgrades. Configuration changes never remove storage.

Programmatic equivalents are `migrateJobs(pool)` and `migrateJobs(pool, 'down')` from the root module.
`installJobs(pool)` is also a convenience for installation. Each operation uses one transaction.
Existing incompatible/unversioned namespaces are rejected by up; down can remove them intentionally.

## Application migration files

```ts
import { jobsMigration } from '@kequtech/pgclear-jobs/migrations';

// Save jobsMigration.up and jobsMigration.down as application migration files.
```

These strings use the same canonical statements and advisory lock as standalone setup. They contain
no outer `BEGIN`/`COMMIT`; the application's migration runner supplies the transaction. Raw up SQL
is an installation migration, applied once by that runner, rather than a repeatable setup command.

Commit concrete SQL files. Do not make a historical migration import the newest package or call a
live installer: dependency updates must not change saved migration behavior. Tenon can copy/apply
this pair without understanding Jobs tables or execution internals.

Workers check a small internal compatibility marker before starting. They never migrate implicitly.
After an uncertain commit, inspect database state; an error alone does not prove rollback.
