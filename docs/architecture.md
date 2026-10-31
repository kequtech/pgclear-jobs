# Architecture and execution

## Sister libraries

| Package                | Responsibility                                    |
| ---------------------- | ------------------------------------------------- |
| `@kequtech/tenon-orm`  | Application queries, transactions, and migrations |
| `@kequtech/tenon-jobs` | Durable job records, job execution, and recovery  |
| `@kequtech/pgclear`    | Shared PostgreSQL driver foundation               |

Keep the published Tenon ORM identity. The family name communicates the relationship between the
higher-level libraries; their names need not mirror the driver's name. A possible future Tenon
asset library is an idea, not part of this project's implementation scope.

Both existing local checkouts and npm packages are available for pgclear and Tenon ORM. Reuse them
and their foundations. Modify their public boundaries only if an actual integration requirement
demonstrates the need.

## Package boundaries

The intended dependency direction is:

```text
tenon-orm ────────────────→ pgclear
tenon-jobs core ──────────→ pgclear
tenon-jobs/tenon ─────────→ Tenon public APIs + jobs core
```

Pgclear remains a normal runtime dependency. Making developers manage the driver as a required
peer dependency is not needed for naming coherence. Sharing a pool is a connection-ownership
decision, separate from dependency installation.

The proposed `@kequtech/tenon-jobs/tenon` entry point owns the integration. Tenon ORM does not need
to import Jobs or advertise methods that fail when Jobs is absent. Tenon imports and types must be
isolated so importing the core package works without Tenon installed. An optional Tenon peer
dependency is the intended packaging mechanism; compatibility ranges remain to be chosen.

Definitions should be shareable without importing the integration. Standalone producers and
runners require Tenon only if their own application imports or handler implementations use it.
Module dependency tests will need to verify this boundary, including emitted declarations.

## Record-operation scope

The core entry point adapts pgclear pools and transactions. The Tenon entry point adapts Tenon
database and transaction query clients. Both expose the same record-operation vocabulary:

```ts
jobs(scope).send(definition, payload, options);
jobs(scope).get(id);
jobs(scope).find(definition, filters);
jobs(scope).reschedule(id, runAt);
jobs(scope).cancel(id);
```

Internally, a narrow parameterized SQL execution boundary isolates the responsibility. It is an
implementation detail of the integration, not an adapter application developers repeatedly write.
The exact internal executor type and query-result representation remain to be specified.

The local Tenon checkout exposes `query` on database and transaction clients, and public
`unsafeSql(text, values)` constructs its SQL object. These APIs appear sufficient for an adapter.
`db.native` is the underlying pool and does not join a Tenon callback transaction; it must not be
used to execute operations requested through `jobs(tx)`.

Operations through a transaction must obey its lifetime checks and fail after it finishes. Queue
facades must not leak caller-owned scopes into runners or deferred work.

## Execution boundary

1. Application code opens its transaction.
2. It writes application data and inserts or changes queue records through that transaction.
3. Commit makes the changes visible. Rollback leaves no inserted job to execute.
4. An independently running consumer claims a committed job in a short database operation.
5. It runs the matching predefined handler outside that claim transaction.
6. Another short operation records the attempt outcome, checking claim ownership.

The request and originating transaction can be long gone before step 4. Neither closures,
executors, database sessions, nor requests are persisted job data. Notifications used to wake
runners must commit with the record operation; polling remains necessary.

There is no special handler transaction API. A handler can use ordinary application database code,
including its own transactions, but Jobs does not coordinate those transactions with execution.

## Runner topology and ownership

`runJobs(pool, definitions)` starts an explicit processing loop in the calling process. It is not a
thread launcher and does not create an external worker service. It can run concurrently with an
application server or be the principal task of a dedicated process.

Each runner selects a set of job names through its supplied definitions. Different selections
provide separate processing groups without separate database tables. Multiple runners selecting
the same definitions must claim safely across processes. Unknown definitions must remain unclaimed
by runners that cannot handle them; visibility should help identify stranded work.

Local concurrency is useful, but configuration placement and defaults remain open. Local limits
apply per runner/process; they do not imply a global limit across all processes. Distributed limits
would be a separate requirement.

No runner starts merely because a module is imported, a definition is constructed, or a job is
submitted. A supplied pool belongs to the application. Shutdown must finish or deliberately release
runner responsibilities before the application closes that pool.

## Shared database ownership

There is one shared set of Tenon Jobs tables per database. Job types, processes, and processing
groups do not create additional installations. All processes must target the same installation.

Tenon Jobs owns its table definitions and migration history. Schema installation and upgrades are
explicit deployment operations, usable independently of Tenon ORM. Normal submission must not
perform DDL or silently upgrade tables inside an application transaction. The migration interface,
schema name, and compatibility rules remain open.
