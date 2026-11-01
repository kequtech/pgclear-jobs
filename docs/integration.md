# Integrating a pgclear-backed query library

`@kequtech/pgclear-jobs/integration` exports `createJobsIntegration`, the `JobsIntegration` contract
and the generic `Execute` SQL capability. An integration lives in the consuming query library and
uses the same Jobs engine as native pgclear applications. No ORM package is installed by Jobs.

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

The example uses a deliberately small query scope. A real query library implements `execute` using
its normal query API and `transaction` using its configured database's transaction API.

## Contract

- `pool(database)` returns the existing application-owned pgclear pool for claims, maintenance and
  scheduling. It must not construct another pool. Pre-aborted runners do not resolve this capability.
- `execute(scope)` returns an executor for that exact database or transaction scope. It must preserve
  the query library's serialization, scope lifetime, parameters and query observation. It resolves
  only when the SQL result is available, and does not commit independently inside a transaction.
- `transaction(database, work, options)` opens a real transaction, supplies its application query
  scope to `work`, and commits only when that callback and accepted SQL have succeeded. A rejection
  rolls it back. Forward cancellation options to pgclear so an ignored handler signal cannot retain
  a usable transaction. Preserve original and aggregate driver errors, including uncertain commits.

The factory returns `defineJob`, `jobs` and `runJobs` bound to these scopes. Transactional handlers
receive the integration's transaction type; native and integrated runners cannot interchange
transactional definitions. Record operations accept either definition while preserving payload
types, since they only read definition metadata. The integration decides what application facade
to expose; it does not change queue policy or reimplement claims/retries.

Completion runs through the transaction executor after the application handler returns. Its database
budget is enforced through the transaction's abort signal, so the adapter does not need a second SQL
timeout API. The engine refuses completion after lost ownership and waits for transaction cleanup.
The original JavaScript handler keeps its local slot until it settles or shutdown abandons it.

## Lifecycle forwarding

Forward the factory's `runJobs(database, definitions, options)` promise directly. pgclear-jobs owns
worker startup, claiming, maintenance, notifications, graceful shutdown, handler cancellation and
driver cleanup. Its runner options and abort signal have the same behavior with native and integrated
scopes. Startup and operational failures reject the worker promise for the application's supervision.

The query library only checks its own enablement/open state and supplies the capabilities above.
The application requests shutdown through the signal and awaits the worker before closing its
database. The Jobs engine leaves the shared pool open. No separate worker/drain implementation
or per-handler lifecycle wrapper is required in the integration.

## Query-library implementation still pending

Tenon ORM will own its integration, forward transaction cancellation options, and expose `db.jobs`
and `tx.jobs`. Those changes are tracked in the [implementation plan](implementation-plan.md).
The former Jobs-owned `/tenon` module and borrowed-pool facade have been removed.

When implementing the ORM facade, port its earlier regression examples from Git commit `3925fa4`,
especially affected-row savepoint recovery after a SQL error, concurrent guarded query ordering,
configured query observation, transaction expiry, producer rollback and notification commit timing.
The core now independently exercises the shared transaction engine using native and custom scopes,
including cancellation, ownership takeover, completion lock waits and lost rollback/commit responses.
