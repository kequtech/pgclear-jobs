# API

Core exports come from `@kequtech/tenon-jobs`. The optional `/tenon` entry point exports
`jobs(scope)` for a Tenon database or transaction; its record API is identical to core pgclear scopes.

## Definitions

```ts
const sync = defineJob({
  name: 'sync-account',
  maxAttempts: 8,
  retryDelayMs: 1000,
  retryMaxDelayMs: 60_000,
  retryJitter: true,
  concurrency: 2,
  async handler(payload: { accountId: string }, context) {
    await synchronize(payload.accountId, context.signal);
  },
});
```

Definitions are immutable and inert. Names contain 1–200 characters and cannot be blank.
Handlers return `void` or `Promise<void>`; persist business results in app-owned records.

| Policy               | Default       | Meaning                                         |
| -------------------- | ------------- | ----------------------------------------------- |
| `maxAttempts`        | 3             | Total attempts including the first; 1–1000      |
| `retryDelayMs`       | 1000          | Base delay; 0–86400000 ms                       |
| `retryBackoff`       | `exponential` | `fixed` or base × 2^(attempt−1)                 |
| `retryMaxDelayMs`    | 86400000      | Delay cap; 0–2147483647 ms                      |
| `retryJitter`        | false         | Choose between half and all of the capped delay |
| `executionTimeoutMs` | 300000        | Attempt deadline; 1–2147483647 ms               |

Optional `concurrency` is 1–1000, per definition **within each runner's** overall limit.
It is not a distributed limit. Submissions snapshot execution policy. Existing work retains its
policy when code changes, while the deployed handler for its name executes it.

The handler context contains `id`, one-based `attempt`, saved `maxAttempts`, cooperative `signal`,
and `scheduledFor` (a Date for a recurring occurrence, otherwise null).

Payloads must be JSON primitives, dense arrays or plain objects. Undefined, nonfinite numbers,
bigint, cycles, Dates, accessors, symbols and other lossy JSON values are rejected before SQL.
Names, keys, payload strings and object keys reject NUL and unpaired Unicode surrogates.
TypeScript does not validate stored payload schemas; validate untrusted data in application code.

## Submission

All methods below belong to `jobs(scope)`. They use that scope and never start or commit a caller's
transaction. `runAt: Date` and `delayMs: number` are mutually exclusive; relative delay uses the
database clock and accepts 0–2147483647 ms. Omitted timing means immediately eligible.

### `send(definition, payload, options?)`

Returns `Job<Payload>`. Options contain timing and optionally **one** of:

- `id`: a UUID. An existing ID returns its original record unchanged, including terminal records.
  IDs are database-wide; reusing another definition's ID raises `JobStateError`.
- `key`: a string up to 500 characters, scoped to the definition name. Pending and running records,
  including retries, occupy the key. A collision returns unchanged work. Terminal records release it.

Without either option, every call creates independent work. A result has no created/existing flag.
ID deduplication lasts only while the original record exists. Neither mechanism deduplicates external
handler effects. Conflict paths can wait for another transaction and hold row locks until commit.

### `sendMany(definition, submissions)`

Each submission is `{ payload, id?, key?, runAt?, delayMs? }` with the same validation and mutually
exclusive options as `send`. The batch accepts 0–1000 submissions; an empty batch returns `[]`.
All inputs are validated before one INSERT. Any database error rolls back the statement.
Duplicate IDs or outstanding keys, including duplicates within the batch, are skipped.
Returns **only inserted** typed jobs; order is not guaranteed. Use supplied IDs to correlate results.

### `upsert(definition, payload, { key, runAt?, delayMs? })`

The key is required. Inserts if it is free; otherwise replaces a pending job's payload and supplied
timing, returning the same ID. Omitted timing preserves the existing execution time. Existing policy,
attempt count and history remain unchanged. Concurrent upserts agree on one outstanding record.
If that record is running, throws `JobStateError` without changing it or aborting the caller's
transaction. This method promises no trailing execution after running work.

## Records and inspection

A `Job<Payload>` contains `id`, `name`, typed `payload`, nullable `key`, `state`, `attempts`,
`policy`, `lastError`, and lifecycle Dates (`runAt`, `createdAt`, `updatedAt`, nullable `finishedAt`).
Occurrence fields are nullable `scheduleId` and `scheduledFor`. `retryOf` links a manually retried
job to its original failure; it can refer to a record later removed by cleanup.
`cursor` is an opaque pagination position preserving database timestamp precision.

| Method                                                 | Result                                                |
| ------------------------------------------------------ | ----------------------------------------------------- |
| `get(id)`                                              | `Job<unknown>` or null                                |
| `find(definition, { key?, states?, limit?, after? }?)` | Typed records in creation-time, then UUID order       |
| `attempts(id, { limit? }?)`                            | Newest attempts first; absent IDs return `[]`         |
| `stats(definition)`                                    | Counts by state, plus `ready`, `delayed`, and `total` |

Lists default to 100 and permit at most 1000. `states: []` matches nothing. An omitted state filter
matches all states. To continue, pass the last returned job's `cursor` as `after`, using the same
definition and filters. Stop when a page is empty. Removing the cursor row does not invalidate its
position. Pagination is a live traversal: concurrent changes are not a frozen snapshot.

Each attempt contains `number`, `startedAt`, nullable `finishedAt`, `outcome`
(`completed`, `failed`, `expired`, or null), and nullable `error`.
Statistics use one current database snapshot, without a cache: `pending` includes both `ready`
and future `delayed` work. Counts describe retained records, not historical throughput.

## Mutations and maintenance

| Method                                           | Contract                                                                 |
| ------------------------------------------------ | ------------------------------------------------------------------------ |
| `update(definition, id, payload)`                | Replace a matching pending payload; returns typed job                    |
| `reschedule(id, runAt)`                          | Set a pending job's execution time; returns job                          |
| `cancel(id)`                                     | Cancel a pending job; returns job                                        |
| `retry(id)`                                      | Create a fresh job from a failed record; returns new job                 |
| `remove(id)`                                     | Delete a terminal job and its attempts; true if removed, false if absent |
| `prune(definition, { before, states?, limit? })` | Delete one bounded batch of terminal records; returns count              |

Missing mutation targets raise `JobNotFoundError`; ineligible targets raise `JobStateError`.
`remove` is idempotent for absent IDs, but refuses pending/running jobs. State conflicts are ordinary
application errors, so a caller can catch them and continue its transaction. Concurrent deletion may
change a diagnostic not-found/state result. IDs and input values are validated before SQL; database
faults, serialization failures and deadlocks propagate without automatic retries.

Manual `retry` copies payload, key, saved policy and schedule context into a **new ID** linked by
`retryOf`, with zero attempts and a fresh policy budget. The original remains failed with its history.
A currently occupied key raises `JobStateError`; the caller transaction remains usable. Update any
app reference to the returned job ID. Repeated unkeyed retry calls request independent jobs;
a lost commit response still needs application reconciliation.

`prune` considers `finishedAt < before`, ordered by finish time and UUID. Defaults are
`states: ['completed', 'cancelled']`, `limit: 100`; the maximum is 1000. Failures require an explicit
`'failed'` selection. Pending/running states are never accepted. Locked rows are skipped; zero can
mean remaining candidates are locked. Deletion cascades to attempt history. No automatic retention
policy deletes app work; schedule explicit cleanup if desired. All mutations join `jobs(tx)`.

## Recurring schedules

```ts
await jobs(pool).schedule(
  cleanup,
  {},
  {
    key: 'hourly',
    cron: '0 * * * *',
    timeZone: 'UTC',
    missed: 'once',
    overlap: 'skip',
  },
);
```

A schedule belongs to a definition name and schedule key (default `''`), distinct from individual
job keys. `schedule` inserts or updates it and returns a typed `Schedule`. An unchanged cron/zone
preserves its cursor; changes start at the next future occurrence. Payload and policy changes apply
to future jobs. Five-field deterministic cron expressions are required; randomized `H` is rejected.

| Option     | Default  | Meaning                                                                        |
| ---------- | -------- | ------------------------------------------------------------------------------ |
| `cron`     | Required | Five-field expression                                                          |
| `timeZone` | `UTC`    | IANA time zone                                                                 |
| `key`      | `''`     | Schedule identity within the definition; up to 500 characters                  |
| `missed`   | `once`   | Submit one saved due occurrence, or `skip` if more than one minute late        |
| `overlap`  | `skip`   | Suppress while that schedule has pending/running work; `allow` permits overlap |

`Schedule` exposes `id`, `name`, `key`, `payload`, `policy`, `cron`, `timeZone`, `missed`, `overlap`,
`nextRunAt` and nullable `lastJobId`. Cleanup can remove the last referenced job.
`schedules(definition, { limit? }?)` lists by key, default 100, maximum 1000.
`unschedule(definition, key?)` returns whether it removed a producer; submitted jobs stay intact.
Schedule writes and removals join caller transactions.

`previewSchedule({ cron, timeZone? }, { from?, count? }?)` returns Dates strictly after `from`
(default now), without a database. Count defaults to 5 and accepts 1–100. Calendar/DST behavior follows
cron-parser; the scheduler preserves the identity of its saved forward-calculated occurrence.

## Runner and installation

`installJobs(pool)` explicitly installs/upgrades the shared schema transactionally. Current schema
version is 4. Run it before producers/consumers start. Runners require the current version.

`runJobs(poolOrConnection, definitions, options?)` runs until aborted or a database fault occurs.
Supply at least one definition with no duplicate names. Only selected names are executed.
A supplied pgclear pool is borrowed; connection strings or `PoolOptions` create an owned pool.

| Runner option        | Default                                                       | Contract                                              |
| -------------------- | ------------------------------------------------------------- | ----------------------------------------------------- |
| `signal`             | None                                                          | Stop new claims and maintenance on abort              |
| `concurrency`        | 1                                                             | Maximum unfinished local handlers; 1–1000             |
| `notifications`      | Enabled for connection settings; disabled for a borrowed pool | Explicit listener connection settings, or false       |
| `pollIntervalMs`     | 30000 with notifications, otherwise 1000                      | Reconciliation backstop                               |
| `recoveryIntervalMs` | 1000                                                          | One recovery and schedule batch per interval          |
| `leaseMs`            | 30000                                                         | Heartbeat lease; minimum 30 ms                        |
| `shutdownTimeoutMs`  | 30000                                                         | Grace before active work is abandoned; zero permitted |
| `databaseTimeoutMs`  | 10000                                                         | Bound driver operations and listener startup          |

Other millisecond options are positive 32-bit integers. For a borrowed pool, explicitly pass
`notifications: connection` to enable its dedicated listener. The listener needs a direct/session-
pooled connection; query traffic may use transaction pooling. App code closes a borrowed pool only
after observing runner shutdown. See [reliability](storage-and-reliability.md) for cancellation limits.
