# pgclear-jobs boundaries and implementation plan

## Decision and current stage

`@kequtech/pgclear-jobs` is the PostgreSQL job engine for pgclear. It must be usable without
Tenon ORM and must contain no Tenon imports, peer dependency, handler wrappers or ORM-specific
configuration. Tenon ORM will depend directly on both pgclear and pgclear-jobs and own the
integration exposed through `db.jobs` and `tx.jobs`.

The rename and pgclear-only implementation are complete. The package now exposes a typed integration
factory and one up/down SQL pair, supports a standalone migrate command, and has no ORM dependency
or `/tenon` entry point. Tenon ORM changes below remain subsequent work; its proposed application
examples are not current APIs. No package publication or production app migration is part of this stage.

The repository becomes `kequtech/pgclear-jobs`, and `kequtech.source` becomes `pgclear-jobs`, matching
the source already renamed in Kequtech. The checkout has also been renamed to `pgclear-jobs`.
The storage namespace is `pgclear_jobs` and
project environment variables use `PGCLEAR_JOBS_`. Existing configured database connection values
are preserved. Older `tenon_jobs` installations are disposable development data, not an upgrade
source; the rename neither migrates nor drops that namespace automatically.

## Package and resource ownership

| Package      | Owns                                                                                                             | Does not own                                                   |
| ------------ | ---------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------- |
| pgclear      | Connections, SQL transport, transaction/savepoint lifetime, cancellation and physical cleanup                    | Job policy, ORM query behavior or application migrations       |
| pgclear-jobs | Worker startup/shutdown, Jobs schema SQL, records, claims, retries, scheduling, retention, fencing and execution | Tenon clients, application pools or application results        |
| tenon-orm    | ORM queries and guards, application migration tooling, the configured database and its Jobs facade               | A second queue engine or copied Jobs schema implementation     |
| Application  | Enabling Jobs, migration execution, lifecycle triggers, process supervision and business idempotency             | Worker shutdown algorithms or per-handler integration adapters |

`@kequtech/pgclear` is a **required peer dependency** of pgclear-jobs, with the same supported range
in `devDependencies` for builds and tests. The caller supplies pgclear capabilities and owns the
pool, so the application should choose a compatible driver version explicitly. A peer declaration
communicates that relationship; it does not guarantee a single physical installation. Keep genuine
private runtime dependencies such as cron-parser as normal dependencies.

Tenon ORM declares both packages as normal dependencies. This satisfies the Jobs peer requirement
for ORM applications without asking them to install or configure a separate integration package.
Keep compatible pgclear ranges aligned. Do not add an ORM dependency or reverse import to Jobs.

Importing either library, defining a job, creating a Tenon database or accessing its Jobs facade
must not install storage, start workers, register schedules or open notification connections.
Workers borrow the configured application pool. Stop and await workers before closing its owner;
the Jobs engine never closes that pool.

## One engine with a small public integration boundary

Keep the standalone API centered on `defineJob`, `jobs(scope)`, `runJobs(pool, definitions, options)`
and explicit installation. Native transactional handlers receive a pgclear `Transaction`.

`createJobsIntegration` is the typed integration factory exported from `pgclear-jobs/integration`.
Its responsibilities
are limited to obtaining the root pgclear pool for queue infrastructure, turning a supplied database
scope into the existing SQL execution capability, and opening a handler transaction with its abort
signal. It returns definitions/record/runner bindings for the supplied transaction type. Keep this
boundary in one module; do not introduce a plugin registry, arbitrary database backend protocol,
global connection or transaction discovery through async context.

The native implementation opens transactions with `pool.transaction`. Tenon's implementation opens
them with the configured `db.transaction`. Both call the same job execution code and complete the
job through an executor for the exact transaction that contains the handler's writes.

The engine must track application handler settlement separately from transaction settlement.
Cancellation can roll back and expire a driver transaction while uncooperative JavaScript continues
to occupy its local slot. Confirmed handler failures are recorded after rollback. Lost ownership
refuses completion and rolls back writes. Unknown commits and rollback/release failures reach
supervision, including during shutdown; no application transaction is silently replayed.

The borrowed-pool facade is removed. No pool proxy, synthetic pool statistics, throwing
session-acquisition methods or newly constructed ORM instance is needed per handler.

## Worker lifecycle stays in pgclear-jobs

Tenon's `db.jobs.run(definitions, options)` delegates directly to the integration's `runJobs` and
returns its promise. Starting that operation starts the worker. `options.signal` requests shutdown;
pgclear-jobs stops claiming, allows active work its configured grace period, then signals remaining
handlers and cancels their transactional scopes. It stops maintenance and notifications, waits for
driver cleanup, and settles the worker promise. Startup, database and cleanup errors reject that
same promise for application supervision. The engine does not close the application-owned pool.

Tenon does not implement another dispatcher, worker registry, drain loop, shutdown timer, listener
manager, retry loop or error translation. Its wrapper checks that Jobs is enabled and the database
is open, then forwards definitions, options and the result. `tx.jobs` never starts workers.

The application chooses when to run or stop workers, connects its existing process signals and
supervision, and closes the database after awaiting worker shutdown. For a dedicated worker entry
point, the proposed ORM API composes as follows:

```ts
const stop = new AbortController();
process.once('SIGTERM', () => stop.abort());
process.once('SIGINT', () => stop.abort());

try {
  await db.jobs.run(definitions, { signal: stop.signal });
} finally {
  await db.close();
}
```

An HTTP application observes the same promise through its existing supervision and requests shutdown
before closing the database. `db.close()` keeps its database-resource responsibility; it does not
need to track workers. Multiple runs remain independently controlled. A later convenience lifecycle
API, if needed, belongs in pgclear-jobs and is forwarded by Tenon.

## Tenon ORM transaction support and public API

Extend `db.transaction(callback, options?)` to forward pgclear transaction options, especially
`signal` and `timeoutMs`, through acquisition, execution and commit. Preserve the driver's cleanup
and uncertain-outcome errors. This is useful ORM functionality independently of Jobs; no pgclear
API change is currently required.

Tenon owns the integration factory binding. Job operations inside an ORM transaction use its normal
query path, sharing serialization with affected-row savepoints and preserving query observation and
scope expiry. Do not unwrap the transaction to a separate pool or bypass that serialization.
The adapter may use the root database's public underlying pool for queue infrastructure.

Re-export the ORM-typed `defineJob`, errors and relevant public types from tenon-orm. Definitions
remain inert and independent of a configured database instance. Application code does not import
pgclear-jobs just to define or handle a job.

Proposed application shape:

```ts
import { createTenon, defineJob } from '@kequtech/tenon-orm';

const db = createTenon({ connection: databaseUrl, jobs: true });
const refreshProject = defineJob({
  name: 'refresh-project',
  transactional: true,
  async handler(payload: { projectId: string }, { tx }) {
    await updateProject(tx, payload.projectId);
  },
});

await db.transaction(async (tx) => {
  await saveProject(tx, project);
  await tx.jobs.send(refreshProject, { projectId: project.id });
});
await db.jobs.run([refreshProject], { signal });
```

`db.jobs` exposes records and worker operations. `tx.jobs` exposes only operations that can join the
transaction; it cannot start workers or install storage. There is no `db.jobs.install()` in the
planned ORM API: application migrations own ORM installation. The standalone Jobs installer remains
available to applications that do not use Tenon.

## Explicit enablement without implicit database changes

Add `jobs?: boolean` to Tenon configuration, defaulting to `false`. It is an application capability
choice in code, not another environment variable. `jobs: true` enables Jobs operations but does not
install a schema or start workers.

Keep `db.jobs` and `tx.jobs` available with stable types. When disabled, their operations fail with
an actionable Jobs-disabled error **before SQL or connection acquisition**, leaving an existing
application transaction usable. Pure job definitions do not depend on enablement. Ordinary ORM
queries must not inspect or require the Jobs schema.

An enabled worker validates schema compatibility at startup and fails clearly if the installation
migration has not been applied. Do not add a schema lookup to every submission. Applications must
deploy the migration before starting enabled producers/workers. Turning the configuration off
preserves all Jobs data; it never means uninstalling storage or cancelling retained work.

## One Jobs schema, integrated with application migrations

pgclear-jobs owns one canonical schema and exports `jobsMigration.up` and `jobsMigration.down` from
`pgclear-jobs/migrations`. Both use the reserved namespace and include the installation advisory
lock, with no outer transaction-control statements. The standalone `pgclear-jobs migrate` command
installs it; `pgclear-jobs migrate down` removes the entire namespace and its data. Repeated up is
safe and preserves existing work. A small internal compatibility marker prevents a worker from
running against incompatible storage. There is no version-range API or incremental migration chain.

Tenon copies these two strings into normal application migration files using its existing directory,
numbering, checksums and execution machinery. Repeated setup should recognize its generated Jobs
migration instead of duplicating it. It must not execute migrations or modify previously generated
files. Applying them remains an ordinary application migration operation.

Historical application migrations contain concrete SQL; they do not call a live installer or import
the newest package during execution. Dependency updates cannot change those saved files. Tenon
needs no knowledge of Jobs tables, index changes, claim ownership, or retry rules.

A severe breaking schema change may use an intentional down/up reset even when queued jobs are lost.
Document that consequence when such a change occurs, but do not build a preserving upgrade engine
in anticipation. Configuration never invokes down implicitly. Stop producers and workers before
removal. There are no additional confirmation flags or empty-table requirements.

## Reliability ownership

pgclear-jobs owns execution, fencing, retries, scheduling, retention, and transaction outcome
handling. Tenon supplies its existing pool and guarded query/transaction scopes through one binding,
then forwards the public operations. Do not duplicate queue state or algorithms in the ORM.

Keep regression coverage for rollback, lease loss, stale completion, ignored cancellation, worker
shutdown, and lost commit/rollback responses in Jobs. Tenon's tests should concentrate on its own
boundary: correct scope and cancellation forwarding, query guards/observation, disabled operation,
and generated migration files. Applications retain business idempotency and supervision duties.

## Implementation sequence

1. **Rename and document this boundary (complete).** Update package/repository/source identity, SQL namespace,
   notification channel, environment names, links, fixtures and CI together. Preserve the existing
   implementation and verify the renamed package. The transitional `/tenon` entry point was removed
   in the completed core refactor below.
2. **Implement the pgclear-only package (complete).** Remove `src/tenon.ts`, its export and every Tenon dependency
   and import from the package and its declarations. Move pgclear to required peer plus development
   dependency. Add the narrow integration factory, one canonical up/down SQL pair and the migrate command.
   Preserve the current queue semantics and reliability behavior. Adapt the packed-consumer check
   to install the required peer explicitly and verify that no ORM dependency is required. Move ORM
   behavioral coverage into the ORM repository when implementing that integration; the former cases
   are identified in the [integration guide](integration.md), and shared native/custom-scope engine tests remain here.
3. **Extend Tenon transactions.** Add options forwarding and tests for cancellation during acquisition,
   execution and commit, expired scopes, affected-row savepoints and preserved cleanup errors. Use
   pgclear's existing capabilities rather than implementing another cancellation system.
4. **Add Tenon's built-in Jobs integration.** Declare direct pgclear-jobs and pgclear dependencies;
   add `jobs: false` by default, inert typed definitions, `db.jobs`, restricted `tx.jobs`, and the
   centralized transaction/executor binding. Forward worker lifecycle unchanged through `db.jobs.run`;
   preserve the worker promise, abort signal and shutdown options. Use local packed artifacts to test the unpublished
   packages together; do not commit machine-specific `file:` dependencies or publish to unblock tests.
5. **Add explicit setup and migration generation.** Reuse Tenon's existing migration creation tools
   and the product setup flow. An explicit enable-Jobs setup operation generates the concrete SQL
   up/down migration pair and tells the developer to enable `jobs: true`; applying it remains a normal migration
   step. No setup action runs just because the dependency is installed.
6. **Validate one private application integration.** Use a locally packed library in a development
   checkout with a disposable database. Exercise supervision, shutdown, rollback, loss of ownership,
   migration replay and disabled operation. Use the findings for the later documentation cleanup.
   A production canary and npm publication are separate decisions.

## Required acceptance checks

- Jobs builds, typechecks and runs as a packed pgclear-only consumer with no Tenon imports in runtime
  code or declarations. The peer requirement and ORM dependency graph are validated from packages.
- Core claim, retry, follow-up, schedule, retention, transport and transactional regressions remain.
  Both integrations preserve atomic handler writes/completion, SQL-error rollback, completion lock
  waits, ownership takeover, cancelled transaction expiry and local slot retention.
- Lost commit responses do not replay app transactions. Lost rollback/release responses still reject
  the worker, even after cancellation, and preserve the original sanitized handler error.
- Normal ORM use with Jobs disabled creates no Jobs storage, workers or listeners. Disabled Jobs
  operations fail before SQL and do not abort an existing application transaction.
- `tx.jobs.send/request` commit and roll back with application writes. Tenon return modes, concurrent
  guarded query serialization, savepoint recovery and configured query observation still work.
- Through `db.jobs.run`, verify pre-aborted startup stays inert, normal abort permits graceful
  completion, grace expiry cancels transactional work even if handlers ignore the signal, and
  startup/driver/cleanup failures reach supervision. The pool remains usable until its owner closes
  it after worker shutdown. These are integration checks of the Jobs engine, not new ORM algorithms.
- Generated installation SQL is accepted by Tenon's existing SQL scanner and migration runner,
  including functions/triggers. Apply it to both a fresh app database and an app with existing
  migrations, verify repeatable setup and checksums, and verify that upgrading the package does not
  alter old migration files. Missing/incompatible storage produces an actionable worker-start error.
- Preserve the existing configured repositories, database connections and application migrations.
  Destructive schema resets happen only through an intentional down operation.

## Main tradeoffs

Tenon will install Jobs code even for applications that disable the feature. In exchange, applications
get one supported API with no integration glue. Keep initialization inert and avoid adding a feature
plugin manager to recover a small amount of unused installed code.

The public integration boundary needs compatible package versions and behavioral tests. Supporting
this one known integration does not require supporting every ORM or driver. Most existing execution
machinery remains necessary because transaction ownership and uncertainty do not disappear behind
a more convenient facade. The simplification is removing duplicate ownership and per-handler wrappers.

If a future schema change requires preserving retained work, assess a dedicated upgrade then.
The current supported model is one schema with explicit installation/removal. Execution reliability
remains the priority; application migration history stays immutable.

## Prerequisites before ORM implementation

No additional pgclear-jobs or pgclear runtime change is currently required. The existing integration
factory covers pool access, SQL execution and real handler transactions. pgclear already supplies
cancellable transactions, expiring scopes, bounded cleanup and uncertain-outcome errors. Tenon must
forward those existing options and preserve its guarded query path. Begin with that ORM change,
then implement the thin Jobs facade and application migration generation, validating with local
packed packages before publication.
