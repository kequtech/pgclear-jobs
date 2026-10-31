# Initial implementation contract

This describes the experimental 0.1.0 implementation. Earlier design documents remain the
planning record; this document resolves the contracts implemented so far.

## Installation and ownership

`installJobs(pool)` explicitly creates the shared `tenon_jobs` schema, migration history,
job records and attempt history in one transaction. An advisory transaction lock serializes
installers. The current schema is version 2; installation upgrades version 1 by adding notification
triggers while preserving records. Newer versions are rejected. Runners require the current schema:
run `installJobs` explicitly before starting them. Producers and runners never install or upgrade
storage themselves. Do not manually create objects in this reserved schema.

All SQL uses pgclear with bound parameters. `/tenon` wraps the same operations with Tenon's
public `unsafeSql` and `query`; it never accesses `db.native`. A facade retains only its supplied
scope, obeys that scope's lifetime and never owns the caller's transaction. Runners borrow a supplied
pool; the application closes it after observing shutdown. Passing connection settings instead lets
the runner create and close its own pool. A notification listener always has its own connection,
created and closed by the runner through pgclear's existing listener API.

## Definitions and payloads

`defineJob({ name, handler, ...policy })` returns an immutable definition without database work.
Names contain 1–200 characters and cannot be blank. A handler receives `(payload, context)`;
context contains `id`, one-based `attempt`, and a cooperative `AbortSignal`.

| Policy               | Default       | Contract                                                     |
| -------------------- | ------------- | ------------------------------------------------------------ |
| `maxAttempts`        | 3             | Total attempts, including the first; 1–1000                  |
| `retryDelayMs`       | 1000          | Base delay, 0–86400000 ms                                    |
| `retryBackoff`       | `exponential` | `fixed` or base × 2^(attempt−1), capped at one day           |
| `executionTimeoutMs` | 300000        | Maximum duration of each attempt, positive 32-bit integer ms |

Submission snapshots policy into the record. Changing a definition affects new submissions;
existing work keeps its policy, but executes the handler deployed under its name. Applications
must maintain payload compatibility or use a new definition name. TypeScript types do not perform
runtime schema validation on stored data. Handlers should validate untrusted payloads.

Payloads must be JSON primitives, dense arrays or plain objects, including `null`. Undefined,
nonfinite numbers, bigint, cycles, Dates and other class instances are rejected before SQL.
PostgreSQL's normal JSONB limits also apply. Data must survive a JSON round trip; no driver-specific
value types are stored in payloads. Timestamps in public records are JavaScript Dates.

## Record operations

`jobs(scope)` offers:

| Method                                         | Result and eligibility                                           |
| ---------------------------------------------- | ---------------------------------------------------------------- |
| `send(definition, payload, { key?, runAt? }?)` | A typed job record; default execution time is the database clock |
| `get(id)`                                      | A job with unknown payload, or `null`                            |
| `find(definition, { key?, states?, limit? }?)` | Typed records, oldest creation first, then UUID                  |
| `update(definition, id, payload)`              | Replace the payload of a pending job matching the definition     |
| `reschedule(id, runAt)`                        | Set a pending job's earliest execution time                      |
| `cancel(id)`                                   | Move a pending job to cancelled                                  |
| `attempts(id, { limit? }?)`                    | Newest attempts first; absent IDs have empty history             |

Lists default to 100 and have a maximum of 1000. There is no pagination yet. A state filter of
`[]` returns no rows; an omitted filter includes all states. Mutations enforce pending state in
their SQL write. Missing IDs throw `JobNotFoundError`; existing but ineligible records throw
`JobStateError`. A mismatched definition on update is ineligible. Mutation errors are ordinary
application errors, so callers may catch them without aborting their SQL transaction. The follow-up
existence check is diagnostic; concurrent deletion could change which error is observed.

A key is a string of at most 500 characters, scoped to the definition name. Pending and running
records occupy keys, including pending retries. Database uniqueness arbitrates collisions.
`send` returns an existing outstanding record with its payload, policy and timing unchanged;
it never reports whether it created the record. Terminal states release the key. The conflict
path performs a no-op row update to return the locked current record, including under contention.
It may wait for another transaction to commit. PostgreSQL serialization/deadlock errors propagate.

Keys are not permanent idempotency keys. If a submission's commit response is lost, inspect state
and use an application reconciliation policy; do not blindly retry unkeyed submissions. Even a
keyed retry can create new work if the original has already completed. No automatic SQL retries
hide uncertain outcomes.

## Execution and recovery

`runJobs(poolOrConnection, definitions, options?)` is a long-running promise. Observe its rejection.
Supply at least one definition and no duplicate names. Only matching names are claimed. A runner can
share the application's event loop or run in a different process; it does not launch threads.
CPU-blocking work should run separately and can still block its own heartbeat.

Claims use a short `FOR UPDATE SKIP LOCKED` statement and a fresh UUID token per attempt. No
transaction or row lock spans a handler. Every completion, failure and renewal checks the current
token and lease; successful completion also checks the execution deadline. History writes and
state transitions happen atomically. Throwing retries according to stored policy; returning
completes the attempt. Error messages are truncated to 4000 characters without stacks or payloads.
Applications must keep credentials and sensitive data out of thrown messages.

Heartbeats renew the lease up to the maximum execution deadline. A runner that loses ownership
signals cancellation and stops recording outcomes. An unfinished handler continues occupying its
local slot after cancellation, timeout or ownership loss, until it actually settles. A handler
ignoring cancellation can therefore stall its runner's slots; use process supervision when needed.
The concurrency limit applies within one runner, including overrunning handlers, not across runners.

Expired attempts recover in batches of 100, return to pending with backoff, or become failed when
their total budget is exhausted. One maintenance loop per runner performs one batch per interval,
independently of occupied handler slots. It can recover all definition names. It runs while a
runner is active; no runner means no processing or recovery. One dispatcher fills available slots
and keeps claiming while work is available, instead of maintaining a polling loop per slot.

| Runner option        | Default                                              | Meaning                                                             |
| -------------------- | ---------------------------------------------------- | ------------------------------------------------------------------- |
| `signal`             | None                                                 | Stop claiming and maintenance on abort                              |
| `concurrency`        | 1                                                    | Unfinished local handlers, 1–1000; no distributed concurrency limit |
| `notifications`      | Connection settings, or disabled for a borrowed pool | A URI/pgclear connection settings for the listener, or `false`      |
| `pollIntervalMs`     | 30000 with notifications, otherwise 1000             | Fallback reconciliation interval                                    |
| `recoveryIntervalMs` | 1000                                                 | One independent recovery batch per interval                         |
| `leaseMs`            | 30000                                                | Renew every third of this interval; minimum 30 ms                   |
| `shutdownTimeoutMs`  | 30000                                                | Grace for active handlers after abort, then abandon                 |
| `databaseTimeoutMs`  | 10000                                                | Bound each driver operation and listener startup                    |

## Discovery and notifications

With connection settings, notifications are enabled by default. With an existing pool, use the
same connection configuration explicitly: `runJobs(pool, definitions, { notifications: connection })`.
Pools do not expose their credentials to Jobs. No extra dependency, environment variable, or
pgclear API change is required. Set `notifications: false` for polling alone.

A database trigger emits the definition name on `tenon_jobs_ready` when a job is inserted as
pending, returns to pending, or changes its pending execution time. This includes future jobs,
so runners can reset their delayed-job timers. It covers core, Tenon, retry and recovery operations
in the same transaction as the write. Rollback sends nothing. Key collisions that leave work
unchanged send nothing. Payloads and job IDs are not broadcast. Runners ignore unrelated names.

Notifications are hints to inspect durable records. Wakeups coalesce, with a generation counter
retaining notifications received between a storage check and sleep. Listener registration and
reconnection trigger reconciliation, covering startup and delivery gaps. pgclear owns reconnect
after an established connection drops. Initial registration failures retry once per second.
Unavailable notifications emit a Node.js warning with code `TENON_JOBS_NOTIFICATIONS_UNAVAILABLE`
and use polling capped at one second until the listener connects. The listener requires a direct
or session-pooled PostgreSQL connection; a separate direct configuration can be supplied when the
query pool uses PgBouncer transaction pooling.

The claim statement also determines the next future execution time from the same database snapshot.
A timer wakes the dispatcher when that work is due, including delayed retries. This avoids a gap
between an empty claim and a separate scheduling query. Polling remains a backstop for missed
notifications and locked work that becomes available without another notification. While every
local slot is occupied, the dispatcher waits for handler settlement rather than repeatedly fetching.

## Shutdown and failures

A job found during an in-flight claim at shutdown is left for lease recovery. Active attempts get
the shutdown grace to finish. On grace expiry, their signals abort and renewal stops; leases expire normally.
Database operations already in progress can add up to their configured timeout to shutdown.
Uncooperative JavaScript and external effects cannot be forcibly stopped. The caller should
terminate a dedicated worker process if process isolation is required.

An execution deadline abandons the claim and recovery records its expiration; the attempt signal
is cooperative. The local slot is held until the handler settles, but another runner can claim a
retry. Shutdown grace expiry releases the runner's wait for even an uncooperative handler. Tokens
protect persisted queue state, never an external effect. Execution remains at least once.

Database faults reject the runner, signal active work, and leave unfinished claims for recovery.
Restart and backoff belong to the application's process supervision. The runner does not swallow
database errors or close a borrowed pool. Owned pools and listeners are disposed before return.

## Limits and next work

Terminal records and attempt history are retained indefinitely. Job records expose state, attempts,
last failure, policy and lifecycle times. A worker registry, overdue-work statistics,
cleanup, retry/removal APIs, schedules and permanent submission idempotency remain future work.

This version has behavioral PostgreSQL tests, including a killed worker process, listener reconnection,
transactional notification delivery and a preserving upgrade from schema version 1. Network partitions,
uncertain commit responses, sustained load, mixed application versions and additional upgrade paths still
need investigation before production use. Only the version 1 to version 2 upgrade has been validated.

Mechanism references: [PostgreSQL locking clauses](https://www.postgresql.org/docs/18/sql-select.html#SQL-FOR-UPDATE-SHARE)
and [pg-boss 12.38.0 plans](https://github.com/timgit/pg-boss/blob/12.38.0/src/plans.ts).
