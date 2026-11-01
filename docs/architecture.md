# Architecture

Tenon Jobs has three public concepts: a definition, a persisted record, and an explicit runner.
Definitions combine a typed handler with fixed execution policy. Record methods use the database
scope supplied by the caller. Runners execute committed work independently.

## Boundaries

```text
tenon-orm ────────────────→ pgclear
tenon-jobs core ──────────→ pgclear
tenon-jobs/tenon ─────────→ Tenon public queries + jobs core
```

The core has no Tenon import or required peer. The optional integration uses Tenon's public `query`
and `unsafeSql`, including transaction lifetime checks; it never escapes through `db.native`.
The `/tenon` entry point re-exports the full API, adapting only `jobs`. A packaged consumer test
verifies core runtime and declarations without Tenon installed.

One reserved `tenon_jobs` schema holds jobs, attempts, schedules and schema version metadata across all
definitions and processes. Schema changes belong to explicit `installJobs` deployment work.
No DDL runs during submission. There is no mutable queue registry or implicit global database.

| Module                   | Responsibility                                                     |
| ------------------------ | ------------------------------------------------------------------ |
| `definition.ts`          | Definitions, policy and lossless input validation                  |
| `records.ts`             | Submission, inspection, pending mutations and explicit maintenance |
| `storage.ts`             | Persisted/public records and precise cursors                       |
| `claims.ts`              | Atomic ownership, heartbeats, outcomes and expired-claim recovery  |
| `runner.ts`, `wakeup.ts` | Dispatch, cancellation, notifications and listener lifecycle       |
| `schedules.ts`           | Calendar calculation and transactional recurring production        |
| `retention.ts`           | Bounded automatic cleanup of completed/cancelled history           |
| `schema.ts`              | Serialized baseline installation and keyed-request SQL             |
| `tenon.ts`               | Adapting a supplied Tenon scope to the same record operations      |

## Execution

Keyed `request` submission uses an installed PostgreSQL function so head locking and successor
coalescing remain atomic through either a pool or application transaction. A terminal-transition
trigger releases successors; claims and wakeup timers select only unblocked work.

An app writes data and jobs in one transaction. After commit, a runner claims a due record in a short
statement, then invokes its handler outside that transaction. A separate fenced statement records
success or failure. The handler may use ordinary app transactions; Jobs does not coordinate them
with queue completion. App results and domain idempotency belong in app records.

A runner shares the app event loop or lives in a separately supervised process. It creates no threads.
One dispatcher rotates eligible definitions as it fills local slots; one maintenance loop recovers claims, advances schedules and
cleans up expired completed/cancelled history.
Per-definition concurrency divides those same slots without a polling loop per worker. Notifications
wake the dispatcher, and a timer tracks the next due job. SQL rows remain authoritative. Claims,
next-due lookup and automatic cleanup use bounded index scans per selected definition. No new queue
policy, reservation API, claim batching or partitioning is needed for backlog fairness.

## API scope and comparison with pg-boss

The comparison below uses upstream pg-boss documentation reviewed on 1 November 2026. Site currently
requests pg-boss `^12.36.0`; current upstream examples may include newer behavior. A smaller exported
API does not by itself make an application easier to operate.

| Workflow                | pg-boss                                                              | Tenon Jobs                                                                         |
| ----------------------- | -------------------------------------------------------------------- | ---------------------------------------------------------------------------------- |
| Ordinary work           | Start one instance, create a named queue, then use `send` and `work` | Define a typed job, use `jobs(scope).send`, and select definitions in one runner   |
| Application transaction | Pass an ORM/transaction adapter in the operation's `db` option       | Use `jobs(tx)` through pgclear or the optional Tenon entry point                   |
| Keyed latest-state work | Choose a queue policy and combine it with keyed submission/upsert    | Use keyed `request`, with one head and one coalesced successor                     |
| Recurring work          | Register a cron schedule on the instance                             | Register a cron schedule through the existing record facade                        |
| Lifecycle               | The instance manages workers and connection setup                    | A runner promise and abort signal use the application pool; a listener is optional |

Sources: pg-boss [getting started](https://pgboss.io/),
[jobs](https://pgboss.io/api/jobs), [workers](https://pgboss.io/api/workers),
[adapters](https://pgboss.io/api/adapters), [queues](https://pgboss.io/api/queues), and
[scheduling](https://pgboss.io/api/scheduling).

pg-boss's ordinary path is already short. Our advantages are typed definitions and direct Tenon
transaction scopes, plus a specific follow-up contract. Explicit runner supervision and shared-pool
lifecycle remain responsibilities for the application author. Automatic completed/cancelled retention removes a separate maintenance task.
They should not be described as automatic simplification or a sufficient reason to migrate.

The application vocabulary is `send` for individual events and `request` for latest-state work.
There is no first-wins keyed submission or pending upsert API. Known records can still be explicitly
edited by ID for operational use. Every runner uses an application-owned pool; polling works without
listener settings, and a dedicated listener is an optional latency optimization. Schedules have one
catch-up/overlap contract. These choices reduce the number of interacting behaviors an application
must choose or understand.

Retry policy, concurrency, execution limits, timing, time zones and retention remain configurable
because applications can reasonably need different behavior. Retries use capped exponential backoff
with jitter by default. Undefined object fields follow normal JSON omission rules, avoiding extra
payload-normalization code for optional TypeScript properties. Cleanup reuses the existing bounded
maintenance loop; it introduces no new scheduler, worker or external service.

Lease deadlines, fencing, conservative cancellation and explicit uncertain-commit reconciliation are
part of the reliability contract. Removing them would weaken guarantees in ordinary database
outages. Keep those internals and their behavioral tests; avoid another recovery framework or
transaction replay. In particular, a database operation timeout bounds a different responsibility
from a worker's locally confirmed lease. Transport tests exercise that distinction without adding
runtime options. Broader queue policies, dependency graphs, result storage and driver/backend variants
remain outside the intended scope.
