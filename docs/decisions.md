# Scope, decisions, and remaining questions

Recorded 31 October 2026. The supplied handoff described an experiment beside pgclear and Tenon.
The subsequent conversation selected the Tenon Jobs identity and refined the API. This document
separates accepted direction from suggestions made along the way.

## Accepted direction

| Decision             | Outcome                                                                              |
| -------------------- | ------------------------------------------------------------------------------------ |
| Identity             | Tenon Jobs, a sister library to Tenon ORM; keep the existing ORM name                |
| Foundation           | Use pgclear directly; standalone execution does not require Tenon ORM                |
| Behavior             | Define name, typed handler, and policy together once                                 |
| Record scope         | Prefer `jobs(db).method(...)` and `jobs(tx).method(...)`                             |
| Transaction boundary | Queue record operations join the supplied transaction; execution is independent      |
| Submission result    | Return the job directly; no `{ job, created }` wrapper                               |
| Lookup               | First-class support for finding existing work; exact query contract remains open     |
| Runner               | Explicit long-running `runJobs(pool, definitions)`; no required public worker object |
| Placement            | Runner can share the application process/event loop or run in another process        |
| Work selection       | Different processes can handle different sets of definitions, or share the same set  |
| Storage              | One shared set of queue tables per database                                          |
| Design constraint    | Simple, coherent implementation; avoid adding a subsystem per useful capability      |

The core entry point and a Jobs-owned `/tenon` integration are the current architectural direction.
Exact exports, compatibility ranges, and internal interfaces still need implementation validation.

## Current proposals requiring a precise contract

- `get`, bounded `find`, and attempt inspection, including record types, filters, ordering, and
  pagination.
- Optional outstanding-work keys scoped by definition name, with `send` returning the existing
  record unchanged on collision. Lifetime and uncertain-commit behavior need resolution.
- Pending-only payload editing, rescheduling, and cancellation, with explicit not-found and
  conflict outcomes. Active-job cancellation remains open.
- Total-attempt limits, retry delays/backoff, execution deadlines, and local concurrency. Defaults,
  option placement, and how definition changes affect existing jobs remain open.
- A runner `AbortSignal` option and bounded graceful shutdown. Fatal runner errors and uncooperative
  handler behavior need specification.
- Logical job states, attempt history, schema naming, table layout, indexes, migration interface,
  cleanup, and compatibility across deployments.

## Useful capabilities without premature subsystems

The motivation includes emails, reminders, reports, upload processing, derived-data refreshes,
external synchronization, and periodic maintenance. Useful features should be assessed against
these workflows, not selected merely to minimize table count or match pg-boss.

| Capability                                   | Current position                                                                          |
| -------------------------------------------- | ----------------------------------------------------------------------------------------- |
| Durable submission, delay, retries, recovery | Required foundations                                                                      |
| Lookup and record management                 | Required application capabilities; exact methods still being refined                      |
| Duplicate outstanding work prevention        | Important; key-based contract proposed                                                    |
| Operational visibility                       | Required outcome; worker registry and historical metrics are design choices               |
| Execution configuration                      | Define behavior in code; persisted operational controls need justification                |
| Recurring schedules                          | Useful candidate; first-release inclusion was suggested but not accepted as a requirement |
| Event subscriptions                          | Possible future convenience; explicit transactional submissions cover basic fan-out       |
| Job dependencies                             | Potentially valuable for pipelines; no workflow or graph API chosen                       |
| Partitioning                                 | Defer until a measured workload warrants its complexity                                   |

A recurring schedule, if included, is a durable producer of ordinary jobs. It must reuse definitions,
record submission, retries, and runners. It needs explicit downtime, duplicate occurrence, overlap,
and time-zone behavior. It must not become a second execution model.

Queue names currently correspond to definition names. A separate mutable queue entity is not
required unless an agreed responsibility needs one. Pausing processing across runners is a possible
operational requirement, not a settled configuration subsystem.

The current initial focus excludes a dashboard, general workflow orchestration, RRULE scheduling,
multiple database backends, distributed rate limits, and broad pg-boss compatibility. This does not
ban future additions justified by concrete use. Enqueueing follow-up jobs alone is not a complete
durable dependency-workflow implementation.

## Earlier approaches not selected

- `pgclear-jobs` was an early name. The Tenon family better matches the intended developer
  experience; pgclear remains the driver foundation.
- Renaming Tenon ORM to match pgclear was considered and not selected.
- Always-present `db.jobs`/`tx.jobs` methods with missing-dependency runtime failures were not chosen.
- Explicit `withJobs(db)` extensions and `jobs.using(tx)` were alternatives; explicit
  `jobs(dbOrTx)` is the preferred record-operation shape.
- `createJobs({ database: db })` with implicit record-operation scope was superseded by that shape.
- `createWorker(...).start()`/`.close()` was simplified to a long-running `runJobs(...)` function.
- Fixed three-table or five-table starting layouts were suggestions, not accepted requirements.
- Separate job definitions and separately supplied handler registrations were rejected. Supplying
  complete definitions to a runner only selects which work that runner handles.
- Passing an originating transaction into a handler, or introducing special transactional
  execution, was explicitly rejected.

## Background and evidence

The user considers pg-boss a good tool. The opportunity is a smaller, understandable API, typed
definitions, and coherent integration with the existing libraries. Atomic enqueueing, notifications,
and recovery are not unique advantages; pg-boss already provides substantial machinery. Performance
or reliability advantages require evidence.

The supplied handoff reports scratch probes on PostgreSQL 16.15 using Tenon 0.4.2 and pgclear 0.3.0.
They demonstrated enqueue commit/rollback with application writes, 100 distinct claims from ten
concurrent claim calls, recovery with a changed token, stale completion affecting no row, and
exclusion of future jobs. Those are primitive feasibility results, not validation of this library.
Another probe of transactional application writes plus completion is not the chosen handler design.

The local checkouts inspected in this conversation were Tenon ORM 0.4.5, depending on pgclear
`^0.3.3`, and pgclear 0.3.3. Earlier probe results must not be represented as current-version tests.
The handoff referenced pg-boss 12.38.0; schema research in this conversation consulted its tagged
TypeScript source. A scratch installation found locally was 12.36.0, not 12.38.0.

Temporary probe paths included `/tmp/tenon-queue-feasibility.mjs`. They may disappear and are not
project-owned tests. Implementation needs its own reproducible validation.

Earlier size and maintenance estimates were speculative and preceded the refined design. They
are neither code-size targets nor schedule commitments. Recovery and database compatibility remain
ongoing responsibilities even with a small interface.

## Next design work

Review the [end-to-end API](api.md) as a whole. Resolve the high-impact contracts for keys, concurrent
mutations, execution policy across deployments, and runner shutdown. Then derive a minimal storage
model and implement a vertical slice of the reminder workflow with behavioral PostgreSQL tests.
Do not expand into additional subsystems before establishing how they fit this model.
