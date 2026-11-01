# Query-library integration

`@kequtech/pgclear-jobs/integration` exports `createJobsIntegration`, `JobsIntegration`, `JobsApi`,
and `Execute`. This is for a pgclear-backed query library to implement once. Ordinary pgclear
applications use the root API directly.

The factory binds definitions, record operations, and the existing runner to the integration's
transaction type. No new pool or per-handler wrapper is needed.

```ts
import type { Pool } from '@kequtech/pgclear';
import { createJobsIntegration } from '@kequtech/pgclear-jobs/integration';
import type { Execute } from '@kequtech/pgclear-jobs/integration';

interface Scope {
  readonly execute: Execute;
}
interface Database extends Scope {
  readonly pool: Pool;
}

const api = createJobsIntegration<Database, Scope>({
  pool: (db) => db.pool,
  execute: (scope) => scope.execute,
  transaction: (db, work, options) =>
    db.pool.transaction(
      (tx) => work({ execute: (sql, values) => tx.queryClassic(sql, values) }),
      options,
    ),
});
```

This example supplies a minimal custom scope. A real query library uses its configured database and
transaction API, preserving its normal query guards and observation.

## Required capabilities

`pool(database)` returns the existing application-owned pgclear pool. Claims, maintenance, and
schedule advancement use it. It must not construct another pool. A pre-aborted runner does not
resolve this capability.

`execute(scope)` returns a generic parameterized SQL executor for that exact root or transaction
scope. It resolves to `{ rows }` after SQL completes. Preserve the query library's serialization,
parameters, result handling, scope expiry, and query observation. Transaction execution must not
escape to a root pool or commit independently.

`transaction(database, work, options)` opens a real transaction and supplies its query-library
scope. Commit only when the callback and accepted work succeed; roll back on rejection. Forward
pgclear transaction options, especially `signal`, through acquisition, execution, and commit.
Preserve driver/aggregate errors, including uncertain commits and cleanup failures.

## Bound API and types

The factory returns:

- `defineJob`: definitions whose transactional handlers receive the supplied `Tx` type.
- `jobs(scope)`: the ordinary record facade using that scope's executor.
- `runJobs(database, definitions, options?)`: the same worker engine with integrated transactions.

`JobDefinition<Payload, Tx>` and `TransactionalHandlerContext<Tx>` are available from the root module.
Record operations accept native or integrated definitions while preserving payload types because
they only need metadata. Runners must match the transaction type expected by transactional handlers.

Successful completion runs through the transaction executor after the handler returns. Its SQL
budget is enforced through the transaction's abort signal, so the integration needs no separate
statement-timeout API. The engine refuses stale completion and waits for transaction cleanup.
Unsettled JavaScript retains its local slot until it settles or shutdown spends its grace.

## Lifecycle

Forward `runJobs` options and its promise directly. pgclear-jobs owns startup, dispatch, heartbeats,
maintenance, notifications, graceful shutdown, handler cancellation, and driver cleanup. Startup and
operational failures reject that promise. The engine leaves the shared pool open.

A query library may check its own enablement/open state before forwarding. The application connects
its process signals and supervision, requests shutdown, awaits the worker, then closes the database.
Keep queue algorithms and worker lifecycle inside pgclear-jobs.

## Tenon ORM

The Jobs package contains no Tenon module or dependency. Tenon's planned `db.jobs`/`tx.jobs` API is
not implemented here. See the [handoff plan](implementation-plan.md) for its binding, cancellation
forwarding, migration generation, and acceptance checks.
