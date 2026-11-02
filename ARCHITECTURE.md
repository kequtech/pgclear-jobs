# Architecture

pgclear-jobs has three main concepts: a typed definition, a persisted job, and an explicit runner.
Producer specifications and definitions are inert. A worker binds executable code to a shared
specification. Record methods use the database scope supplied by the caller. Runners execute
committed work and own their worker lifecycle.

## Boundaries

| Component                 | Responsibility                                                                               |
| ------------------------- | -------------------------------------------------------------------------------------------- |
| Application               | Business effects/idempotency, when workers run, process supervision, pool ownership          |
| pgclear-jobs              | Jobs SQL, claims, execution, retries, leases, scheduling, retention, worker startup/shutdown |
| pgclear                   | Connections, SQL transport, transaction/savepoint lifetime, cancellation, driver cleanup     |
| Query-library integration | Supply the existing pool, exact query scope, and real transaction opener                     |

There is no ORM dependency, global pool, mutable queue registry, or per-handler adapter. The public
integration factory binds another pgclear-backed query API to the same engine. Tenon ORM implements its facade
without copying queue logic.

One reserved `pgclear_jobs` schema stores jobs, attempts, schedules, and a compatibility marker.
Installation/removal is explicit. One concrete up/down SQL pair serves both the standalone command
and application-owned migration files. Storage changes ship separate explicit upgrade SQL; there is
no automatic incremental migration engine.

## Execution path

A producer submits through `jobs(pool)` or `jobs(tx)`. Transactional notifications appear with the
committed work. The runner selects due jobs using short atomic claims and fresh ownership tokens,
then invokes handlers outside the claim statement.

An ordinary handler has a separate completion statement. A transactional handler uses a fresh real
transaction for its application writes and fenced successful completion. Handler errors roll back
before failure/retry bookkeeping. Managed-transaction commit and cleanup failures propagate to supervision;
exceptions from independently opened application transactions remain handler failures under retry policy.

Handler settlement and transaction settlement are tracked separately: a cancelled driver scope can
roll back while uncooperative JavaScript continues and retains its local slot. `attempt.ts` owns
execution and [hook semantics](docs/reference.md#observation-and-error-policy); the runner owns dispatch,
maintenance, notifications, and shutdown.

A terminal trigger promotes keyed successors atomically. A request function handles head/successor
contention within the caller's SQL scope. Recurring schedules submit ordinary jobs or keyed requests
in bounded transactions. Automatic cleanup uses a completed/cancelled-only index and bounded batches.

## Source layout

| Module                                                  | Responsibility                                                                        |
| ------------------------------------------------------- | ------------------------------------------------------------------------------------- |
| `definition.ts`, `validation.ts`                        | Typed definitions, execution policy, input validation                                 |
| `records.ts`, `storage.ts`                              | Public record operations, mapping, precise pagination cursors                         |
| `claims.ts`                                             | Atomic claims, renewals, outcomes, expired-claim recovery                             |
| `observation.ts`                                        | Local event isolation and typed handler-error policy                                  |
| `attempt.ts`                                            | One handler attempt: lease/deadline monitoring, transaction, completion, cancellation |
| `runner.ts`, `wakeup.ts`                                | Dispatch, maintenance, notification lifecycle, abortable waits                        |
| `schedules.ts`, `retention.ts`                          | Recurring production and automatic history cleanup                                    |
| `schema-sql.ts`, `schema.ts`, `migrations.ts`, `cli.ts` | One schema and explicit setup/removal                                                 |
| `integration.ts`, `pgclear.ts`                          | Query/transaction bindings and the native binding                                     |

The [reliability contract](docs/reliability.md) explains failure boundaries. The
[integration guide](docs/integration.md) defines the query-library contract and its acceptance checks.
