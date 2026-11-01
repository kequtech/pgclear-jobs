# API

Import from `@kequtech/pgclear-jobs`. pgclear is a required peer dependency; the package contains no
ORM adapter. No record facade opens or commits a caller's transaction. See the [integration API](integration.md)
for other pgclear-backed query scopes and the [quick start](../README.md) for ordinary use.

## Definitions

```ts
const sync = defineJob({
  name: 'sync-account',
  async handler(payload: { accountId: string }, { signal }) {
    await synchronize(payload.accountId, signal);
  },
});
```

Definitions are immutable and open no connections. Names contain 1–200 characters and cannot be
blank. Handlers return `void` or `Promise<void>`; business results belong in application records.

| Definition option    | Default        | Meaning                                                 |
| -------------------- | -------------- | ------------------------------------------------------- |
| `maxAttempts`        | 3              | Total attempts, including the first; 1–1000             |
| `executionTimeoutMs` | 300000         | Per-attempt execution limit                             |
| `concurrency`        | Runner's limit | Per-definition cap within each runner                   |
| `transactional`      | false          | Commit handler writes and completion in one transaction |
| `retryDelayMs`       | 1000           | Base delay; 0–86400000 ms                               |
| `retryBackoff`       | `exponential`  | `exponential` or `fixed`                                |
| `retryMaxDelayMs`    | 86400000       | Cap before jitter                                       |
| `retryJitter`        | true           | Randomize between half and all of the capped delay      |

Execution limits and retry policy are saved with each new job. Deployed handlers run existing jobs
with their saved policy; new policy settings apply to new records. Definition concurrency is local
to each runner, not a distributed limit. Its range is 1–1000.

Handler context contains `id`, one-based `attempt`, saved `maxAttempts`, cooperative `signal`, and
`scheduledFor` (a Date for a recurring occurrence, otherwise null).

Throw normally to retry. `new JobError(message, { retry: false })` fails permanently.
`new JobError(message, { retryAt: date })` honors a provider's retry time without shortening policy
backoff or extending the attempt budget.

Payloads are JSON primitives, dense arrays or plain objects. Undefined object fields are omitted
without mutating the original payload. Top-level undefined, undefined array elements, nonfinite
numbers, bigint, cycles, Dates, accessors, symbols and other lossy values are rejected before SQL.
Names, keys and JSON text reject NUL and unpaired Unicode surrogates. Validate untrusted application
payloads in application code; TypeScript does not validate previously stored JSON schemas.

### Transactional handlers

Set `transactional: true` on a definition to receive `tx` alongside the ordinary handler context.
`tx` is a pgclear `Transaction`, including `query`, `queryClassic` and `savepoint`. Use `jobs(tx)`
for transactional submissions. Definitions produced by an integration receive that integration's
transaction type; its runner supplies that scope to the same execution engine.

Claims commit before the handler transaction opens. Application writes through `tx`, follow-up
submissions, successful attempt history and job completion then commit together. Handler exceptions
roll them back before failure/retry bookkeeping is recorded separately. Completion rechecks the
attempt token, lease and deadline; refused completion rolls back application writes. Deadline, lost
ownership and shutdown expiry cancel the driver transaction even if the handler ignores its signal.
Its local slot remains occupied until the original handler settles or shutdown spends its grace.
The supplied transaction expires after use and must not escape the handler.
Its lifetime, including acquisition and commit, is bounded by the attempt's lease/execution signal;
`databaseTimeoutMs` bounds the runner's queue statements and maintenance operations.

Every operation requiring this atomicity must use `tx`. Calls through another database/pool,
separate transactions and external effects do not participate. Await all transactional operations.
This does not prevent duplicate business submissions or make external effects exactly once.

Transactional execution is a property of the deployed handler, like its implementation, and is not
saved in job policy. It applies to existing jobs when that handler is deployed. It is opt-in and holds
one pool connection for the transaction's duration; allow additional connections for heartbeats,
claims, recovery and application queries. Keep transactions short. Commit/transport errors reject
the runner for supervision; cancellation does not suppress rollback or connection-release failures.
If cleanup fails after a handler error, the original sanitized error remains available as its cause.
No application transaction is automatically replayed. A lost commit
response leaves both writes and completion committed, or neither, and recovery observes that state.

## Submit work

Submission methods accept `runAt: Date` or `delayMs: number`, not both. Relative delay uses the
PostgreSQL clock. Omitted timing makes new work immediately eligible. Millisecond timing options
accept positive 32-bit integers; `delayMs` also accepts zero. Unknown submission options are rejected.

### `send(definition, payload, { id?, runAt?, delayMs? }?)`

Creates an independent job and returns `Job<Payload>`. Every call creates work unless an optional
stable UUID already exists. Reusing that UUID returns its original record unchanged in any state,
including completed. Reusing an ID from another definition raises `JobStateError`.

Stable IDs identify retained submissions; cleanup releases their deduplication. Retain the same UUID
outside an uncertain transaction when reconciling a lost response. `send` has no keyed deduplication
mode: use `request` when the desired work represents the latest state of an entity.

### `request(definition, payload, { key, runAt?, delayMs? })`

Returns the pending `Job<Payload>` for the required key (up to 500 characters, scoped to the
job definition). If no work exists, inserts it. If pending work has never started, replaces its
payload and supplied timing. Omitted timing preserves an existing job's execution time.

Once any attempt has begun, leaves the current job and its automatic retries unchanged and retains
one pending successor. Later requests replace that successor's payload and supplied timing. It has
its own ID, saved execution policy and fresh attempt budget. A `blockedBy` ID marks it while waiting.
Completion, permanent failure, exhausted recovery, or cancellation during retry backoff releases it
atomically. Its requested execution time is preserved; both the time and predecessor termination
must permit execution. Further requests can create another successor once it starts.

There is at most one head and one blocked successor per definition/key. Concurrent requests use
row-lock serialization order. Multiple requests coalesce; each call does not promise a separate run.
Payloads are replaced rather than merged. Preserve domain rules such as a sticky force flag in
application-owned state. This contract orders valid claims; stale handlers can outlive their lease.

Use ordinary `get`, `find`, `update`, `reschedule`, and `cancel` methods for successors. Cancelling a
blocked successor leaves its predecessor alone and permits a replacement. All changes join the
caller's transaction. Keep transactions short: a conflicting request can delay a heartbeat.

### `sendMany(definition, submissions)`

Accepts up to 1000 `{ payload, id?, runAt?, delayMs? }` entries. Validates every entry before one
INSERT. Duplicate IDs, including duplicates within the batch, are skipped. Returns only inserted
jobs, with no order guarantee. Supply IDs to correlate input and output. An empty batch returns `[]`.
Use individual `request` calls for keyed coalescing.

## Inspect and operate

A `Job<Payload>` contains `id`, `name`, typed `payload`, nullable `key`, `blockedBy`, `state`,
`attempts`, saved `policy`, `lastError`, and lifecycle Dates (`runAt`, `createdAt`, `updatedAt`,
nullable `finishedAt`). Nullable occurrence fields are `scheduleId` and `scheduledFor`.
`retryOf` links a manual retry to the original failure; cleanup can remove that original later.
`cursor` is an opaque pagination position preserving PostgreSQL timestamp precision.

| Method                                          | Result                                                                |
| ----------------------------------------------- | --------------------------------------------------------------------- |
| `get(id)`                                       | `Job<unknown>` or null                                                |
| `find(def, { key?, states?, limit?, after? }?)` | Jobs in creation-time/UUID order                                      |
| `stats(def)`                                    | Retained counts by state, plus `ready`, `delayed`, `blocked`, `total` |
| `attempts(id, { limit? }?)`                     | Latest attempts first; absent IDs return `[]`                         |
| `update(def, id, payload)`                      | Replace a pending payload                                             |
| `reschedule(id, runAt)`                         | Set a pending execution time                                          |
| `cancel(id)`                                    | Cancel pending work                                                   |
| `retry(id)`                                     | Create a new job from a failed record                                 |
| `remove(id)`                                    | Delete terminal history; true if removed, false if absent             |
| `prune(def, { before, states?, limit? })`       | Delete a bounded terminal-history batch                               |

Lists default to 100 and permit at most 1000. Continue `find` with the last returned job's `cursor`
as `after`, keeping the same filters. A removed cursor row remains usable. Pagination is a live
traversal, not a frozen snapshot. `states: []` matches nothing. Attempts expose `number`, `startedAt`,
nullable `finishedAt`, `outcome` (`completed`, `failed`, `expired`, or null), and nullable `error`.

Statistics use one database snapshot. `pending` is the sum of ready, future delayed and blocked
work. A blocked job is counted as blocked regardless of its requested execution time. Counts
represent retained records, not historical throughput.

Mutations refuse ineligible states with `JobStateError` and missing targets with `JobNotFoundError`.
These errors leave the caller's transaction usable. Cancellation never revokes running external
effects. Explicit pending edits can change retry payloads; ordinary `request` preserves them.
Manual retry creates a fresh UUID and budget, preserving original history and schedule context;
`retryOf` links the records. An occupied key or a pending/running job for the same schedule raises
`JobStateError`. Repeated unkeyed, unscheduled manual retries
can create independent work; reconcile a lost response through application state.

## Retention

Runners automatically remove completed and cancelled history older than seven days, for their
selected definitions. Cleanup also removes attempts and releases stable-ID deduplication. Failures,
pending jobs, running jobs and blocked successors are retained. `retentionMs` on the runner extends
this window or disables automatic cleanup with `false`. Prefer the same setting across runners
selecting the same definition; the shortest configured retention can delete shared history.

Manual `prune` uses `finishedAt < before`; defaults are `states: ['completed', 'cancelled']` and
`limit: 100`, maximum 1000. Include `'failed'` explicitly to prune failures. Nonterminal states are
rejected. Locked records are skipped. `remove` and `prune` join caller transactions.

## Recurring schedules

```ts
await jobs(pool).schedule(cleanup, {}, { cron: '0 * * * *' });
```

Options are required five-field `cron`, optional IANA `timeZone` (default `UTC`), and optional
schedule `key` (default empty string, at most 500 characters). Multiple keys allow multiple schedules
for a definition. Randomized `H` cron expressions are rejected.

Unchanged cron/zone preserves the next occurrence across startup registration. Changes start at the
next future occurrence. Payload and policy updates apply to future jobs. After downtime, submit one
saved occurrence and advance into the future. A schedule suppresses new occurrences while its own
work remains pending/running, including automatic and manual retries. Each schedule has its own
identity; different schedules for the same definition can run independently. A runner selecting
the definition must be running to advance its schedules.

`Schedule` exposes `id`, `name`, `key`, `payload`, `policy`, `cron`, `timeZone`, `nextRunAt` and nullable
`lastJobId`. `schedules(def, { limit? }?)` lists by key (default 100, maximum 1000).
`unschedule(def, key?)` removes the producer, leaving its jobs intact.
`previewSchedule({ cron, timeZone? }, { from?, count? }?)` returns future Dates without a database
(default from now, count 5, maximum 100). Calendar/DST behavior follows cron-parser.

## Installation and workers

`pgclear-jobs migrate` installs the reserved schema using `DATABASE_URL` from the environment or
`.env`. Repeated installation preserves existing jobs. `pgclear-jobs migrate down` removes the
entire namespace, including all Jobs data. Stop producers and workers before removal.

The programmatic equivalents are `migrateJobs(pool)` and `migrateJobs(pool, 'down')`.
`installJobs(pool)` is also available for programmatic installation. Each operation is atomic and
serialized with other migrations. Workers check an internal compatibility marker and never run DDL.
There is one schema definition, with no incremental history or version targeting.
See [migrations](migrations.md) for extracting its up/down SQL.

`runJobs(pool, definitions, options?)` runs until aborted or a database fault occurs. Supply an
application-owned pgclear pool and at least one definition, without duplicate names. Only selected
definitions are executed. Observe this promise and supervise failures. Abort through `signal`, then
close the application pool after the runner settles. The runner never closes the pool.

Each runner rotates successful claims between eligible definitions. Within a definition it selects
oldest due work by execution time/UUID, skipping locked rows. An old backlog in one definition does
not have to drain before another receives a turn. Fairness concerns admission to available local
slots; it does not preempt active handlers or impose a distributed ordering between runners.
Use per-definition concurrency limits or separate runners when long work must leave capacity for
short, latency-sensitive work.

Worker concurrency is independent of pool size. Handlers normally run outside queue transactions,
but their own SQL, application transactions, claims, renewals and maintenance share the pool.
Keep transactions short and size/monitor the pool for that combined SQL workload. Pool waits spend
lease and operation budgets; exhaustion can cancel a handler and lead to recovery/retry.

| Common runner option | Default            | Meaning                                                                      |
| -------------------- | ------------------ | ---------------------------------------------------------------------------- |
| `signal`             | None               | Stop claims and maintenance, allowing active work its shutdown grace         |
| `concurrency`        | 1                  | Maximum unfinished handlers in this runner; 1–1000                           |
| `notifications`      | None               | Optional dedicated listener connection string or pgclear connection settings |
| `retentionMs`        | 604800000 (7 days) | Completed/cancelled history window; `false` retains it indefinitely          |

The defaults suit ordinary use. Advanced timing controls exist for database latency and execution
coordination: `pollIntervalMs` (1000, or 30000 with notifications), `recoveryIntervalMs` (1000),
`leaseMs` (30000, minimum 30), `shutdownTimeoutMs` (30000, zero allowed), and `databaseTimeoutMs`
(10000). These accept positive 32-bit integers unless noted. Retention accepts positive safe integer
milliseconds, including windows longer than 24 days.

The optional listener requires a direct/session-pooled connection; query traffic may use transaction
pooling. Listener failure falls back to polling. Without listener settings, polling needs no additional
connection configuration. See [reliability](storage-and-reliability.md) for lease and cancellation limits.
