# Integrate another query API

[Overview](../README.md) · [Reference](reference.md) · [Reliability](reliability.md)

Native pgclear applications use the package root. A pgclear-backed query library can bind its own
transaction type once through `@kequtech/pgclear-jobs/integration`:

```ts
import type { Pool } from '@kequtech/pgclear';
import { createJobsIntegration, type Execute } from '@kequtech/pgclear-jobs/integration';

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
      (tx) => work({ execute: (text, values) => tx.queryClassic(text, values) }),
      options,
    ),
});
```

## Binding contract

| Binding                            | Required behavior                                                                    |
| ---------------------------------- | ------------------------------------------------------------------------------------ |
| pool(database)                     | Return the exact application-owned pgclear pool; never construct another             |
| execute(scope)                     | Execute finished SQL/values on that exact root or transaction scope; return {rows}   |
| transaction(database,work,options) | Open a real managed transaction, pass its query scope and forward TransactionOptions |

The adapter must preserve cancellation, scope expiry and original driver/aggregate errors. Do not
swallow infrastructure failures, replay transactions or return a root client as a transaction. The
engine uses the pool for claims, renewals and maintenance and the supplied execute/transaction for
submissions and handler bookkeeping. A pre-aborted runner resolves without requesting its pool.

## Returned API

- defineJob and defineWorker infer the integrated handler transaction type.
- jobs(scope) returns record operations using that exact scope.
- runJobs(database,definitions,options?) returns the engine's lifetime promise.
- startJobs(database,definitions,options?) returns its ready/done handle.

Record operations accept native or integrated specifications; executable registrations must match
the runner's transaction type. Keep definitions/imports inert. A facade can enforce its own database
and scope lifetime before forwarding, including empty batches that perform no SQL. Transaction
facades should expose record operations only, not worker startup or installation.

Forward runner options, handles, errors and events. Queue algorithms, retry timestamps, hook dispatch,
readiness and shutdown remain in Jobs. The application owns process signals and pool close.
Guards/observations specific to the query API remain in that query library. Schema installation and
upgrade strings belong to `/migrations`; integrating them with an app migration runner does not move
queue DDL policy into that runner.

Use one compatible pgclear resolution across the application, Jobs and adapter. Test custom-scope
handlers here; the consuming query library tests its guards, observations and facade lifetime.
[Tenon's integration guide](https://github.com/kequtech/tenon-orm/blob/main/docs/jobs.md) describes its
ORM-specific surface. Jobs has no Tenon dependency.
