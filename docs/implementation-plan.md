# Tenon ORM integration

pgclear-jobs is the pgclear-only Jobs engine. It has no ORM dependency. Its current public API and
integration factory are implemented. Tenon ORM implements the facade described here; this document
records its ownership boundaries and acceptance checks.

## Boundaries

| Component    | Owns                                                                                                |
| ------------ | --------------------------------------------------------------------------------------------------- |
| pgclear      | Connections, transaction/savepoint scopes, cancellation, transport, and driver cleanup              |
| pgclear-jobs | Jobs schema, worker startup/shutdown, claims, execution, retries, schedules, retention, and fencing |
| Tenon ORM    | Its query guards/observation, configured database, thin Jobs facade, and migration generation       |
| Application  | Enablement, lifecycle triggers, process supervision, business idempotency, and pool ownership       |

Tenon depends directly on pgclear and pgclear-jobs, with compatible driver ranges. pgclear remains a
required Jobs peer. Ordinary ORM use with Jobs disabled must create no Jobs storage, workers, or
listeners. Importing libraries and defining jobs remain inert.

## Integration responsibilities

1. **Forward transaction options.** `db.transaction(callback, options?)` and its internal
   wrapper forward existing pgclear transaction options, especially `signal` and `timeoutMs`,
   through acquisition, execution, and commit. Preserve original/aggregate driver errors.
2. **Bind once.** Tenon uses `createJobsIntegration` from `pgclear-jobs/integration` with three capabilities:
   the existing root pool, SQL through the exact ORM query scope, and real ORM transactions.
   Preserve affected-row savepoints, serialized guarded operations, query observation, and scope
   expiry. Do not construct pools or ORM instances per handler or bypass guards via a raw transaction.
3. **Expose the facade.** Tenon re-exports the ORM-typed `defineJob` and `defineWorker`, Jobs errors,
   and relevant types. Shared handler-free contracts belong in producer modules; worker modules bind
   handlers with the ORM transaction type. Tenon exposes `db.jobs` for record/worker operations and
   restricted `tx.jobs` for transactional record operations. Transaction scopes cannot start workers
   or install storage.
4. **Enable explicitly.** `jobs?: boolean` defaults to false. Keep stable facade types; disabled
   operations reject before SQL/acquisition and leave an existing transaction usable. Enabling Jobs
   enables its operations without creating tables or starting workers.
5. **Generate migration files.** An explicit Jobs setup operation copies `jobsMigration.up/down`
   into a normal app migration pair using Tenon's existing directory/numbering/checksum machinery.
   Repeated setup recognizes its generated migration. Do not run it implicitly or edit old files.
6. **Validate packages together.** Use local packed artifacts during development, or an explicitly
   selected published version. Do not commit machine-specific `file:` dependencies.
   An ordinary release/publication and a production pilot remain separate decisions.

## Worker lifecycle

`db.jobs.run(definitions, options)` checks enablement/open state and forwards the integration's
`runJobs`, its options, and its result. Jobs owns dispatch, maintenance, notifications, graceful
shutdown, cancellation, and cleanup. Tenon adds no dispatcher, worker registry, drain loop, shutdown
timer, readiness probe, or retry/error translation. Multiple runs remain independently controlled.
Forward shared callback options and types unchanged. Their contracts live in the API reference:
[startup readiness](api.md#startup-readiness) and
[observation and error policy](api.md#observation-and-error-policy).

An HTTP app observes the same worker promise through its existing supervision, requests shutdown,
and awaits it before closing the database. `db.close()` keeps its database-resource responsibility.
Any future lifecycle convenience belongs in Jobs and can be forwarded by Tenon.

## Schema ownership

There is one canonical schema and one exported `jobsMigration.up/down` pair. Tenon needs no knowledge
of tables, indexes, claim tokens, or queue recovery. Generated SQL executes inside the application's
migration transaction and includes the Jobs advisory lock. Historical migrations contain concrete
SQL, not imports of the newest package or calls to a live installer.

A severe breaking change may use an intentional down/up reset, losing queued work and history.
There is no preserving upgrade engine or version-range API to integrate. Configuration never invokes
down. Stop producers/workers before removal and retain saved app migration files unchanged.

## Acceptance checks

- Cancellation during acquisition, active SQL, callback wait, and commit preserves driver outcomes.
  Expired scopes reject later queries; rollback/release failures remain visible.
- Handler writes, follow-up submissions, and completion commit together. Exceptions and lost ownership
  roll back; ignored handler signals do not retain a usable transaction.
- `tx.jobs.send/request` share app commit/rollback and notification timing. Preserve affected-row
  savepoint recovery, concurrent guarded query ordering, return modes, and `onQuery` observation.
- Through `db.jobs.run`, check inert pre-aborted startup, graceful completion, grace expiry, and
  propagation of startup/database/cleanup failures. Jobs leaves the pool usable until its owner closes it.
- Producer contracts preserve payload types, worker bindings supply the ORM transaction scope, and
  handler-error decisions see original errors after rollback. Uncertain commits must not emit false
  outcome events or enter the ordinary handler-error policy.
- Disabled Jobs operations fail before SQL and do not poison an app transaction. Ordinary queries
  require no Jobs storage. Missing/incompatible storage gives a useful worker-start failure.
- Generated up/down files pass Tenon's SQL scanner and migration runner, work beside existing app
  migrations, and are not duplicated or rewritten by repeated setup/dependency updates.
- Run an actual private application workflow with idempotency, cancellation, supervision, shutdown,
  and delayed/retrying work before relying on production behavior.

Tenon owns regression coverage for affected-row savepoints, guarded concurrency, query observation,
scope expiry, and its producer/notification boundary. Jobs retains native/custom-scope engine
coverage, including readiness. Keep configuration, repositories, connections, and unrelated
application work intact.
