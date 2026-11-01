# API reference

Import definitions, record operations, errors, setup functions, and the runner from
`@kequtech/pgclear-jobs`. Import SQL from `/migrations` and query-library bindings from `/integration`.
The package is ESM and requires a compatible `@kequtech/pgclear` peer.

## Definitions

```ts
const refresh = defineJob({
  name: 'refresh-project',
  async handler(payload: { projectId: string }, { signal }) {
    await refreshProject(payload.projectId, signal);
  },
});
```

`refreshProject` is application code. Definitions are immutable and do not connect or submit work.
Names must contain 1–200 characters and cannot be blank. Unknown definition options are rejected.
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

Calls through other pools, separate transactions, and external effects do not participate. No
application transaction is automatically replayed. Database/commit/cleanup errors reject the worker;
an uncertain commit may have persisted both writes and completion, or neither.
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

| Method                                          | Result                                                           |
| ----------------------------------------------- | ---------------------------------------------------------------- |
| `get(id)`                                       | `Job<unknown>` or `null`                                         |
| `find(def, { key?, states?, limit?, after? }?)` | Jobs in ascending creation-time/UUID order                       |
| `stats(def)`                                    | Counts by state, plus `ready`, `delayed`, `blocked`, and `total` |
| `attempts(id, { limit? }?)`                     | Latest attempts first; missing jobs return `[]`                  |
| `update(def, id, payload)`                      | Replace the payload of a pending job of that definition          |
| `reschedule(id, runAt)`                         | Set a pending job's execution time                               |
| `cancel(id)`                                    | Cancel pending work and release any successor                    |
| `retry(id)`                                     | Create a fresh job from a failed record                          |
| `remove(id)`                                    | Delete a terminal record; `false` if absent                      |
| `prune(def, { before, states?, limit? })`       | Delete a bounded terminal-history batch                          |

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

`schedule(definition, payload, { cron, timeZone?, key? })` returns a `Schedule<Payload>`. Cron must be
a deterministic five-field expression; randomized `H` expressions are rejected. Time zone defaults
to `UTC`; keys default to empty and have at most 500 characters.

Unchanged cron/zone preserves the cursor across registration. Changing them chooses the next future
occurrence. Payload/policy updates apply to future jobs. After downtime, submit one saved occurrence
and advance into the future. Suppress new occurrences while work from that schedule is pending or
running, including automatic/manual retries. Different schedules can run independently.

`Schedule` contains `id`, `name`, `key`, `payload`, `policy`, `cron`, `timeZone`, `nextRunAt`, and
nullable `lastJobId`. `schedules(def, { limit? }?)` lists by key, default 100, maximum 1000.
`unschedule(def, key?)` removes the producer and leaves its jobs intact.

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
| `signal`             | None                                  | Request shutdown and stop new claims/maintenance                       |
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

## Schema setup

`pgclear-jobs migrate` installs storage; `pgclear-jobs migrate down` intentionally removes it.
Both use `DATABASE_URL` from the environment or `.env`. Programmatic equivalents are
`migrateJobs(pool)` and `migrateJobs(pool, 'down')`; `installJobs(pool)` also installs.

There is one schema, with no incremental history or version targeting. Installation is atomic,
serialized, and repeatable. A worker checks compatibility but never applies DDL.
`jobsMigration.up/down` from `/migrations` are the same concrete SQL for application migration files.
See [schema setup](migrations.md) for removal and transaction ownership.
