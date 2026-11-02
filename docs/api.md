# API reference

Import definitions, record operations, errors, setup functions, and the runner from
`@kequtech/pgclear-jobs`. Import SQL from `/migrations` and query-library bindings from `/integration`.
The package is ESM and requires a compatible `@kequtech/pgclear` peer.

## Producer contracts and workers

`defineJob<Payload>({ name, ...policy })` returns an immutable `JobSpec<Payload>` without a handler.
`JobSpecOptions` accepts the name and execution/retry policy fields. Producers can share this small
module without importing application handlers. All record/schedule operations accept specifications
as well as existing inline definitions, preserving payload typing.

`defineWorker(spec, { handler, transactional?, concurrency? })` returns an executable
`JobDefinition<Payload, Tx>`. The handler payload is inferred from the specification; transactional
context uses native pgclear or the integration's transaction type. Worker options cannot change the
name or submission policy. `WorkerOptions` is exported for reusable worker factories. Passing an
inline definition as a specification also works; only its metadata is reused.

Runners require executable definitions; handler-free or malformed registrations are rejected before
pool access. Existing `defineJob({ name, handler, ...options })` calls remain supported.

## Definitions

```ts
const refresh = defineJob({
  name: 'refresh-project',
  async handler(payload: { projectId: string }, { signal }) {
    await refreshProject(payload.projectId, signal);
  },
});
```

`refreshProject` is application code. Definitions and producer specifications are immutable and do
not connect or submit work. Names must contain 1–200 characters and cannot be blank. Unknown
definition options are rejected.
Handlers return `void` or `Promise<void>`; persist business results in application records.

| Option               | Default         | Meaning                                                |
| -------------------- | --------------- | ------------------------------------------------------ |
| `maxAttempts`        | `3`             | Total attempts, including the first; 1–1000            |
| `retryDelayMs`       | `1000`          | Base delay; 0–86400000 ms                              |
| `retryBackoff`       | `'exponential'` | `'fixed'` or `'exponential'`                           |
| `retryMaxDelayMs`    | `86400000`      | Delay cap before jitter; zero allowed                  |
| `retryJitter`        | `true`          | Randomize between half and all of the capped delay     |
| `executionTimeoutMs` | `300000`        | Per-attempt execution deadline                         |
| `concurrency`        | Runner's limit  | Per-runner cap for this definition; 1–1000             |
| `transactional`      | `false`         | Supply a transaction and commit writes with completion |

Execution/retry policy is saved with each new job. Changing a definition does not change existing
jobs' saved policy. `concurrency`, `transactional`, and the handler implementation are runtime
settings. Definition concurrency is local to each runner.

Handler context contains `id`, one-based `attempt`, saved `maxAttempts`, `signal`, and
`scheduledFor: Date | null`. The signal aborts on execution timeout, lost ownership, worker failure,
or shutdown grace expiry. Pass it to cancellable dependencies.

### Transactional handlers

Set `transactional: true` to receive `context.tx`, a pgclear `Transaction`. Await all operations on
that scope. `jobs(tx)` can submit follow-up work in the same transaction.

The claim commits before this transaction opens. Handler writes, follow-up submissions, successful
attempt history, and completion commit together. Handler errors roll back before failure/retry
bookkeeping. Refused completion after lost ownership also rolls back. The driver expires and rolls
back the scope on cancellation even when the JavaScript handler ignores its signal.

Calls through other pools, separate transactions, and external effects do not participate. Failures
opening, committing, or cleaning up the Jobs-managed transaction reject the worker; an uncertain
managed commit may have persisted both writes and completion, or neither.
See [reliability](storage-and-reliability.md).

### Errors from handlers

Ordinary exceptions retry according to saved policy. `JobError` refines that behavior:

```ts
throw new JobError('No longer applicable', { retry: false });
throw new JobError('Provider is busy', { retryAt: new Date(Date.now() + 60_000) });
```

`retryAt` is a minimum time; backoff can delay it further. It does not extend the attempt budget.
A permanent error cannot also specify `retryAt`. The constructor accepts `cause` through standard
`ErrorOptions`. Persisted diagnostics contain sanitized, bounded error text; keep secrets out of it.

Unless an [explicit handler-error policy](#observation-and-error-policy) stops the worker, an exception
escaping the handler follows this retry policy even when it is a driver error from an independently
opened application transaction. Such a transaction may already have committed.
Reconcile durable application state before repeating effects or compensating external work. The
stronger worker-rejection rule above concerns Jobs-managed transaction operations outside handler
invocation; it is not a rule that every thrown `DriverError` bypasses handler retries.

## Record scope

`jobs(pool)` uses the supplied pgclear executor. `jobs(tx)` joins that transaction and expires with
it. The facade never opens or commits a transaction behind the caller's scope. A query-library
integration supplies the equivalent exact scope.

Payloads are JSON primitives, dense arrays, or plain objects. Undefined object fields are omitted.
Top-level undefined, undefined array elements, nonfinite numbers, bigint, Dates, cycles, accessors,
and other lossy values are rejected. Text cannot contain NUL or unpaired Unicode surrogates.
TypeScript payload types are not runtime validation for persisted or untrusted data.

## Submission

### `send(definition, payload, { id?, runAt?, delayMs? }?)`

Create an independent job and return `Job<Payload>`. Without `id`, a fresh UUID is generated. Reusing
an ID returns its original record unchanged in any state while retained. An ID belonging to another
definition raises `JobStateError`. There is no key option on `send`.

### `sendMany(definition, submissions)`

Submit up to 1000 `{ payload, id?, runAt?, delayMs? }` entries in one INSERT after validating the
whole batch. Duplicate IDs are skipped, including duplicates within the input. Return only inserted
jobs, with no ordering guarantee. Supply IDs to correlate results. An empty batch returns `[]`.

### `request(definition, payload, { key, runAt?, delayMs? })`

Coalesce work under a required key, scoped to the definition, and return the affected pending job.
Keys may be empty and have at most 500 characters.

- With no outstanding head, create one.
- For an unstarted head, replace its payload and any explicitly supplied timing.
- Once any attempt has started, preserve that job and its retries; create or update one blocked
  successor instead. That successor has its own ID and saved policy.

Omitted timing preserves an existing record's execution time. Terminal completion, permanent
failure, exhausted recovery, or cancellation during backoff releases the successor atomically. Its
requested time still applies. Requests coalesce in database serialization order; payloads are
replaced, not merged. Sticky force flags and other business arbitration belong in application state.

A key permits one head and one blocked successor. This orders valid claims; an expired handler can
still be running. Cancelling a blocked successor leaves its predecessor alone and permits a replacement.

### Timing

Use either `runAt: Date` or `delayMs`, never both. Relative delay uses the PostgreSQL clock. Omitted
timing makes new work immediately eligible. `delayMs` accepts zero. Unknown submission options fail
before SQL. Keep a stable submission UUID outside an uncertain transaction when reconciling a lost
response; deleting its record releases deduplication.

## Inspection and operations

A `Job<Payload>` exposes:

| Fields                                          | Type/meaning                                                               |
| ----------------------------------------------- | -------------------------------------------------------------------------- |
| `id`, `name`, `payload`                         | UUID, definition name, typed JSON payload                                  |
| `key`, `blockedBy`                              | Nullable key and predecessor UUID                                          |
| `state`                                         | `'pending'`, `'running'`, `'completed'`, `'failed'`, or `'cancelled'`      |
| `attempts`, `policy`, `lastError`               | Attempt count, saved policy, nullable error text                           |
| `runAt`, `createdAt`, `updatedAt`, `finishedAt` | Dates; only `finishedAt` is nullable                                       |
| `scheduleId`, `scheduledFor`, `retryOf`         | Nullable occurrence identifiers/date and original failure UUID             |
| `cursor`                                        | Opaque creation-time/UUID position preserving database timestamp precision |

`retryOf`, `blockedBy`, and `lastJobId` identifiers are not substitutes for retaining related records.
A referenced terminal record can later be removed.

| Method                                          | Result                                                                 |
| ----------------------------------------------- | ---------------------------------------------------------------------- |
| `getMany(ids)`                                  | Found records once each, in first-requested order; missing IDs omitted |
| `get(id)`                                       | `Job<unknown>` or `null`                                               |
| `find(def, { key?, states?, limit?, after? }?)` | Jobs in ascending creation-time/UUID order                             |
| `stats(def)`                                    | Counts by state, plus `ready`, `delayed`, `blocked`, and `total`       |
| `attempts(id, { limit? }?)`                     | Latest attempts first; missing jobs return `[]`                        |
| `update(def, id, payload)`                      | Replace the payload of a pending job of that definition                |
| `reschedule(id, runAt)`                         | Set a pending job's execution time                                     |
| `cancel(id)`                                    | Cancel pending work and release any successor                          |
| `retry(id)`                                     | Create a fresh job from a failed record                                |
| `remove(id)`                                    | Delete a terminal record; `false` if absent                            |
| `prune(def, { before, states?, limit? })`       | Delete a bounded terminal-history batch                                |

`getMany` accepts at most 1000 UUIDs in one statement; it normalizes duplicate IDs and returns
`Job<unknown>` records. Empty input returns `[]` without SQL; invalid IDs and oversized batches
reject before SQL. It uses the caller's transaction, as `get` does.

Lists default to 100 and permit 1–1000. Continue `find` with the last job's `cursor` and the same
filters. Removed cursor rows remain usable. Pagination is live, not a frozen snapshot. `states: []`
matches nothing. Attempts expose `number`, `startedAt`, nullable `finishedAt`, nullable `error`, and
`outcome: 'completed' | 'failed' | 'expired' | null`.

Statistics use one snapshot. Pending includes ready, delayed, and blocked jobs. Readiness also
requires an unblocked due job; counters describe retained records, not lifetime throughput.

Pending edits include retry-backoff and blocked records. They preserve policy, history, and budget.
`retry` preserves its failed source and copies its payload, policy, key, and occurrence context into
a new linked record with a fresh budget. An outstanding key or schedule blocks retry. Repeating an
unkeyed manual retry can create multiple jobs.

Missing mutation targets raise `JobNotFoundError`; ineligible state, wrong definition, or retry
conflicts raise `JobStateError`. Both expose `jobId`. `remove` rejects outstanding records. `prune`
uses `finishedAt < before`, defaults to completed/cancelled states and a limit of 100, and permits
at most 1000. Include failed explicitly to delete failures. Locked rows are skipped. Deleting a job
also deletes its attempts. All operations use the caller's scope.

## Recurring schedules

`schedule(definition, payload, { cron, timeZone?, key?, requestKey? })` returns a `Schedule<Payload>`. Cron must be
a deterministic five-field expression; randomized `H` expressions are rejected. Time zone defaults
to `UTC`; keys default to empty and have at most 500 characters.

Unchanged cron/zone preserves the cursor across registration. Changing them chooses the next future
occurrence. Payload/policy updates apply to future jobs. After downtime, submit one saved occurrence
and advance into the future. Suppress new occurrences while work from that schedule is pending or
running, including automatic/manual retries. Different schedules can run independently.

`Schedule` contains `id`, `name`, `key`, nullable `requestKey`, `payload`, `policy`, `cron`, `timeZone`, `nextRunAt`, and
nullable `lastJobId`. `schedules(def, { limit? }?)` lists by key, default 100, maximum 1000.
`unschedule(def, key?)` removes the producer and leaves its jobs intact.

Set `requestKey` to make each due tick issue a coalesced request, using the same definition/key
as manual or startup requests. This is separate from the schedule identity `key`. It replaces an
unstarted payload or creates/updates one successor behind a started head and all its retries.
Omitted timing preserves existing due times, so ticks never postpone a pending head or successor.
New requests are immediately eligible. Missed ticks coalesce to one submission before advancing
the cursor into the future. Payloads replace rather than merge; saved job policy stays unchanged.

A shared job has no exclusive schedule owner: its `scheduleId` and `scheduledFor` remain null.
The schedule's `lastJobId` records the affected shared job, including an existing one. Two schedules
may point at the same job. Unscheduling leaves jobs intact; manual retry follows normal key
arbitration. Changing `requestKey` changes future ticks without cancelling previous work.
Omitting it restores independent occurrences; changing only key/payload/policy keeps the due cursor.

`previewSchedule({ cron, timeZone? }, { from?, count? }?)` returns future Dates strictly after `from`
without a database. Defaults are now and 5 occurrences; maximum count is 100. Calendar and DST
calculation follows cron-parser. Runners advance only schedules for their selected definitions.

## Runner

`runJobs(pool, definitions, options?)` returns a promise for the worker's lifetime. Supply at least
one definition with no duplicate names. Call it to start; abort `signal` to stop. A pre-aborted call
opens no sessions. Observe rejection for startup, database, and cleanup failures. After shutdown,
await that promise before closing the application pool. The runner never closes the pool.

| Option               | Default                               | Meaning                                                                |
| -------------------- | ------------------------------------- | ---------------------------------------------------------------------- |
| `onEvent`            | None                                  | Best-effort local observation; not awaited                             |
| `onHandlerError`     | Continue                              | Explicit handler-error decision after managed rollback                 |
| `signal`             | None                                  | Request shutdown and stop new claims/maintenance                       |
| `onReady`            | None                                  | Once after storage validation; awaited before dispatch                 |
| `concurrency`        | `1`                                   | Maximum unfinished handlers, including cancelled ones; 1–1000          |
| `notifications`      | None                                  | Dedicated listener connection string or pgclear connection options     |
| `retentionMs`        | `604800000`                           | Completed/cancelled history window; `false` disables automatic cleanup |
| `pollIntervalMs`     | `1000`, or `30000` with notifications | Fallback reconciliation interval                                       |
| `recoveryIntervalMs` | `1000`                                | Maintenance interval, independent of handler slots                     |
| `leaseMs`            | `30000`                               | Ownership lease; minimum 30 ms; renew every third of the lease         |
| `shutdownTimeoutMs`  | `30000`                               | Grace for active handlers; zero allowed                                |
| `databaseTimeoutMs`  | `10000`                               | Queue statement, maintenance, listener startup, and completion budget  |

Unless specified otherwise, millisecond options are integers from 1 to 2147483647.
`retryMaxDelayMs`, `delayMs`, and shutdown grace allow zero. `retentionMs` allows any positive safe
integer. Unknown runner options are rejected.

Claims rotate between eligible definitions; within each definition, due time and UUID order apply,
skipping locked rows. Limits and fairness apply per runner, with no global concurrency guarantee.
Timed-out handlers retain their slot until they settle or shutdown spends its grace. Reserve capacity
for short work using per-definition caps or separate runners.

Automatic retention affects only selected definitions. Failed/outstanding jobs remain. Runners
sharing definitions should agree on retention; the shortest window can delete shared history.
A notification connection requires direct/session pooling and sits outside the query pool.
Listener failures warn and fall back to polling. See [reliability](storage-and-reliability.md).

### Observation and error policy

`RunnerOptions` includes `RunnerHooks`. Neither hook receives an attached job payload or transaction.
Errors retain their original identity and may contain application data; redact them before logging
and do not mutate them. The related types are exported from the package root.

`onEvent?: (event: RunnerEvent) => void | Promise<void>` is best-effort and is not awaited. Throws
and rejections warn with `PGCLEAR_JOBS_OBSERVER_FAILED` without changing job outcomes. Unresolved
observer promises cannot hold slots or prevent shutdown; synchronous work still occupies the event loop.

| Event `type`       | Meaning                                                                                               |
| ------------------ | ----------------------------------------------------------------------------------------------------- |
| `attemptStarted`   | This runner is invoking the handler after acquiring its execution scope                               |
| `attemptCompleted` | PostgreSQL acknowledged completion, including managed commit when applicable                          |
| `attemptFailed`    | Failure bookkeeping was acknowledged; includes original `error` and saved `retryAt`                   |
| `attemptAbandoned` | No outcome was confirmed after local cancellation; includes `reason`                                  |
| `runnerFailed`     | Startup/runtime/cleanup failed; emitted once with the final error before the lifetime promise rejects |

Attempt events carry `AttemptInfo`: `id`, `name`, `attempt`, `maxAttempts`, and nullable `scheduledFor`.
`retryAt` is the saved Date including backoff/jitter, or null for terminal failure. Abandonment reasons
are `leaseLost`, `deadlineExceeded`, and `runnerStopped`. `runnerFailed` may include the originating
`attempt`; concurrent failures are preserved in an `AggregateError`. Identity metadata and Dates are
detached from execution metadata.

Events describe this runner, not expired-claim recovery or other runners, and are not durable.
Process death can lose events, and abandonment before invocation can have no start event. Uncertain
commits emit no successful outcome; confirmed completion can precede a cleanup failure. Stored
jobs/application records remain authoritative. Invalid configuration rejects without promising an event.

`onHandlerError?: (failure: HandlerFailure) => HandlerErrorAction | Promise<HandlerErrorAction>` runs
for an escaping handler exception, after confirmed Jobs-managed rollback and before failure
bookkeeping. `HandlerFailure` contains `AttemptInfo`, original `error: unknown`, the attempt `signal`,
and `transactional: boolean`. Managed commit/rollback/cleanup failures bypass this hook and reject
the worker directly.

- `continue` (the default) preserves ordinary `JobError` and saved retry behavior; it does not force
  another attempt for a permanent or exhausted job.
- `stop` rejects with the original thrown value before failure bookkeeping, leaving the claim for
  lease recovery rather than permanently pausing or failing the job.
- A thrown/rejected policy or invalid JavaScript return value rejects with an `AggregateError`
  containing both the handler and policy errors.

Asynchronous decisions are awaited while ownership is maintained. Deadline, lease loss, and shutdown
interrupt the wait, prevent stale bookkeeping, and observe late rejections. The callback can outlive
the wait, so honor its signal for side effects. Independently opened application transactions may
already have committed; this hook adds no atomicity to them. See [reliability](storage-and-reliability.md).

TypeScript 6 async callbacks returning string literals may need an explicit return annotation
(or `as const`) to preserve strict action checking:

```ts
async onHandlerError(failure): Promise<HandlerErrorAction> {
  await recordFailure(failure); // Application code; honor failure.signal.
  return 'continue';
}
```

### Startup readiness

`onReady?: () => void | Promise<void>` separates startup notification from the
promise representing the worker's lifetime. The callback runs once after schema compatibility is
checked and before dispatch, maintenance, or the optional listener starts. It is awaited; do not
wait for a job to complete inside the callback. A thrown/rejected callback error rejects the worker
with the original value. A pre-aborted worker or an abort during validation never announces readiness.

Readiness means compatible storage was checked and dispatch is about to start. It does not confirm
notification connectivity, every future SQL permission, or ongoing health. Continue observing the
worker promise and route later failures to application supervision.

`startJobs(pool, definitions, options?)` returns a `RunnerHandle` with `ready` and `done`
promises. It uses the same runner/options; `runJobs` remains available. `ready` resolves after
validation and successful completion of `onReady`, before dispatch. Startup errors reject both
promises with the original error. Shutdown before readiness rejects `ready` with an `AbortError`
and normally resolves `done`; a later runner failure rejects `done` without changing `ready`.
Both promises have immediate rejection observation, but the application must supervise `done`.
The handle installs no signal handlers and never closes the pool.

```ts
const worker = startJobs(pool, definitions, { signal });
superviseWorker(worker.done); // Application-owned failure/shutdown policy.
await worker.ready;
// Start accepting HTTP traffic here.
```

Aborting while an asynchronous callback is pending stops startup without waiting for it. The callback
can outlive the worker; its own side effects must honor application cancellation. A later rejection
is observed and does not become an unhandled rejection or restart dispatch. Tenon's `db.jobs.run`
forwards this same option; it needs no separate readiness implementation.

## Schema setup

`pgclear-jobs migrate` installs storage; `pgclear-jobs migrate down` intentionally removes it.
Both use `DATABASE_URL` from the environment or `.env`. Programmatic equivalents are
`migrateJobs(pool)` and `migrateJobs(pool, 'down')`; `installJobs(pool)` also installs.

Storage version 2 adds scheduled request keys. There is no automatic upgrade history or version targeting. Installation is atomic,
serialized, and repeatable. A worker checks compatibility but never applies DDL.
`jobsMigration.up/down` from `/migrations` are the same concrete SQL for application migration files.
`jobsUpgrade` exports explicit version-1-to-2 SQL. See [schema setup](migrations.md) for upgrades,
grants, removal and transaction ownership.
