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
A packaged consumer test verifies core runtime and declarations without Tenon installed.

One reserved `tenon_jobs` schema holds jobs, attempts, schedules and migration versions across all
definitions and processes. Schema changes belong to explicit `installJobs` deployment work.
No DDL runs during submission. There is no mutable queue registry or implicit global database.

| Module                   | Responsibility                                                     |
| ------------------------ | ------------------------------------------------------------------ |
| `definition.ts`          | Definitions, policy and lossless input validation                  |
| `records.ts`             | Submission, inspection, pending mutations and explicit maintenance |
| `storage.ts`             | Persisted/public records and precise cursors                       |
| `claims.ts`              | Atomic ownership, heartbeats, outcomes and expired-claim recovery  |
| `runner.ts`, `wakeup.ts` | Dispatch, cancellation, notifications and connection ownership     |
| `schedules.ts`           | Calendar calculation and transactional recurring production        |
| `schema.ts`              | Versioned, serialized installation                                 |
| `tenon.ts`               | Adapting a supplied Tenon scope to the same record operations      |

## Execution

An app writes data and jobs in one transaction. After commit, a runner claims a due record in a short
statement, then invokes its handler outside that transaction. A separate fenced statement records
success or failure. The handler may use ordinary app transactions; Jobs does not coordinate them
with queue completion. App results and domain idempotency belong in app records.

A runner shares the app event loop or lives in a separately supervised process. It creates no threads.
One dispatcher fills local slots; one maintenance loop recovers claims and advances schedules.
Per-definition concurrency divides those same slots without a polling loop per worker. Notifications
wake the dispatcher, and a timer tracks the next due job. SQL rows remain authoritative.

## Relationship to pg-boss

The review on 1 November 2026 used pg-boss's 12.38.0 package metadata and current upstream sources.
Its jobs API covers bulk insertion, explicit IDs, upsert, retries and removal. We cover these common
workflows with one record API. Our outstanding key spans pending and running work; pending upsert
therefore reports an active conflict. Manual retry creates a linked fresh job, retaining the original
failure, rather than extending that record's retry allowance. See the upstream
[job API](https://github.com/timgit/pg-boss/blob/master/docs/api/jobs.md) and
[retry SQL](https://github.com/timgit/pg-boss/blob/master/src/plans.ts).

pg-boss also combines polling with optional LISTEN/NOTIFY. Our notification hints, delayed timers
and polling backstop serve the same discovery responsibility. See its
[worker API](https://github.com/timgit/pg-boss/blob/master/docs/api/workers.md).

Its queue policies cover distributed exclusivity and ordered keys; these are different from our
local concurrency limits. Our statistics are direct retained-record counts and cleanup is explicit,
without a queue monitor or automatic deletion policy. See its
[queue API](https://github.com/timgit/pg-boss/blob/master/docs/api/queues.md).

Full API parity is not the goal. Dependency graphs, mutable queue policies, stored handler outputs,
dead-letter routing, priority/rate-limit systems and multiple database backends are outside this
library's current model. Failed records already retain diagnostics; app records hold business output.
Concrete pending adoption work and further case assessments are in [todo.md](../todo.md).
