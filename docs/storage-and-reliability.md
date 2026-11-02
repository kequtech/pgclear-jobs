# Reliability and operations

Execution is at least once. PostgreSQL owns durable state; handlers own application effects.
Fenced completion protects queue records from stale attempts. It cannot undo an email, stop arbitrary
JavaScript, or prevent a second business event from being submitted.

## State and ownership

```text
pending → running → completed
             ├──→ pending (automatic retry)
             └──→ failed
pending → cancelled
blocked successor → eligible pending (when its predecessor terminates)
failed → fresh pending record (explicit manual retry)
```

A claim selects due, unblocked work with `FOR UPDATE SKIP LOCKED`, changes its state, assigns a fresh
token, and inserts attempt history in one SQL statement. The claim commits before invoking a handler.
Every renewal and outcome checks the current token and a live lease. Successful completion also
checks the execution deadline. Checks run after obtaining the row lock, so time spent waiting cannot
authorize a stale outcome.

The runner renews ownership while a handler is active. Claim and renewal round trips consume a
conservative local budget measured with a monotonic clock. A late response cannot start an expired
attempt or extend locally assumed ownership. PostgreSQL time governs the stored lease; large clock
changes can expire work. Pass the handler's abort signal through external dependencies.

One maintenance loop per runner recovers expired claims, advances schedules, and prunes eligible
history. Recovery handles up to 100 expired claims per pass, across all definitions, independently
of available handler slots. Retry/error history and state change together.

A cancelled handler retains its local slot until it settles or worker shutdown spends its grace.
Another worker can recover its expired claim in the meantime. Concurrency limits therefore bound
unfinished JavaScript in one runner, not all execution across the deployment. Avoid blocking the
Node event loop, and supervise workers that can stall in uncooperative dependencies.

## Transaction boundaries

`jobs(tx)` uses the caller's exact transaction. Application writes and submitted jobs commit or roll
back together. Jobs and their notifications become visible only after commit. The facade does not
open a hidden transaction or silently retry an application transaction.

An ordinary handler's writes and queue completion are separate operations. A process can fail after
its writes commit but before completion; recovery may run it again. Use application idempotency or
opt into a transactional handler for short database work.

With `transactional: true`, the runner opens a cancellable transaction and passes it as `tx`:

- Writes through `tx`, follow-up submissions, successful attempt history, and completion commit together.
- Handler exceptions roll back application writes before failure/retry bookkeeping is recorded separately.
- Lost ownership or refused completion rolls back the transaction.
- Deadline and shutdown cancellation expire the driver scope even if the handler never returns.
- Commit and rollback/release failures reach worker supervision. Expected cancellation is suppressed
  only when driver cleanup succeeds.

Only operations on that supplied scope participate. Other pools and external calls remain separate.
Retain idempotency for effects outside the transaction and for distinct submissions of the same
business operation. Await every transaction operation and do not retain the scope after use.

By default, exceptions escaping a handler become ordinary attempt failures under saved retry policy.
This includes a driver error from a transaction the application opens independently inside the handler.
The driver does not replay transactions, but a retry of the whole handler can repeat that application
logic. Failures opening, committing, or cleaning up the Jobs-managed transaction outside handler
invocation instead reach worker supervision. A driver error's class alone does not select that path.

Transactional handlers hold a pool connection and application locks until commit/rollback. Claims,
heartbeats, maintenance, and other application work need additional connections. Keep transactions
short; an external call inside one also holds those resources while waiting.

## Uncertain responses

A rejected operation does not prove rollback: PostgreSQL may have committed while its response was
lost. At the record API boundary, pgclear errors propagate without automatic transaction replay.
Using that API inside a handler still subjects escaping exceptions to the handler retry policy.
An uncertain Jobs-managed commit reports `PGCLEAR_COMMIT_UNKNOWN` to supervision; managed cleanup
can also produce aggregate errors. Preserve those infrastructure errors in the supervising code.

For submissions, generate a stable UUID outside the uncertain transaction, retain it, and reconcile
with `send`/`get`. The original record deduplicates that ID while retained. This reconciles the job,
not unrelated application writes: those need their own durable business identifier/state check.
Repeating an unkeyed send or manual retry with a fresh ID can create additional work.

For a transactional handler's uncertain commit, application writes and successful completion are
both committed, or neither. A replacement worker observes database state. A completion whose reply
was lost is not replayed merely because its original worker rejected.

## Keyed requests and schedules

`request` uses one installed PostgreSQL function to serialize the head and coalesced successor.
Requests preserve a started head through all automatic retries. A terminal-transition trigger
releases its successor in the same transaction; rollback restores the relationship. Claims and
wakeups exclude blocked records. This guarantees trailing work, not one execution per request.
Concurrent requests follow database serialization order; application force/timing rules are not merged.

Schedules are durable producers of ordinary jobs. Each maintenance pass locks up to 50 due schedules
for selected definitions, inserts work, and advances cursors atomically. One catch-up occurrence is
submitted after downtime; skipped ticks are not replayed. Occurrence uniqueness prevents duplicate
original submissions, and a unique partial index permits one pending/running job per schedule.
This arbitration includes manual retries. A conflicting retry raises `JobStateError`; a conflicting
scheduler insertion is skipped. Removing a schedule leaves its jobs intact.

Cron/DST calculation follows cron-parser. No matching runner means no schedule advancement. Register
unchanged schedules safely at startup; changed cron/zone starts from a new future cursor.

## Dispatch and notifications

Runners rotate between eligible definitions. Indexed lookups preserve oldest-due/UUID ordering
within each definition and skip locks. Fair admission does not preempt long-running handlers or
create capacity; reserve slots for short work with definition caps or separate runners.

Polling works without another connection. Optional notifications use one dedicated direct or
session-pooled connection per runner, outside the query pool. A trigger sends definition names,
never payloads, after transactional pending changes. Future jobs also notify so timers can adjust.
Wakeup generations and a shared claim/next-due snapshot close check/sleep races.

Listener startup failure retries, and established listeners reconnect. Failure warns with
`PGCLEAR_JOBS_NOTIFICATIONS_UNAVAILABLE` and falls back to polling. Reconnection triggers another
storage check. Polling defaults to one second, or 30 seconds with notifications; while a configured
listener is unavailable, reconciliation is capped at one second.

## Shutdown and supervision

`onReady` notifies a host after storage compatibility is checked, before background work starts.
The runner awaits the callback; the lifetime promise remains the source of later failures. Startup
readiness does not establish listener connectivity or ongoing queue progress. Abort during a pending
callback stops startup and observes any late rejection. See [startup readiness](api.md#startup-readiness).

Call `runJobs` to start. Observe its promise immediately, through an awaited worker entry point or
an application's existing supervisor. Abort its signal to stop new claiming and maintenance.
Active work gets `shutdownTimeoutMs` to finish; then handler signals abort and renewals stop.
The runner closes its listener and waits for driver cleanup. An in-flight claim returned after stop
is left for recovery. The application pool remains open.

Grace expiry bounds waiting for arbitrary handlers, not total wall-clock shutdown. In-flight database
operations can add their remaining timeout and cleanup time. JavaScript that ignores cancellation
may outlive the worker promise. Use process supervision/isolation when that matters.

Runner infrastructure and Jobs-managed transaction faults stop the runner, cancel active work, and
reject its promise. A supervisor can restart it; expired claims recover according to saved policy.
Ordinary handler exceptions follow their retry policy, including errors from independently managed
transactions. Stop and await workers before closing their pool. The query-library integration forwards
this lifecycle without a second shutdown implementation.

## Retention and resource limits

Completed/cancelled records expire after seven days by default. Cleanup checks once a minute and
removes at most 100 records per pass, skipping locks; full batches drain on later maintenance passes.
A dedicated partial index excludes retained failures from cleanup scans. Cleanup affects only the
runner's selected definitions. Sharing definitions with different windows lets the shortest window
remove history. `retentionMs: false` disables automatic cleanup.

Failed and outstanding work is retained. Use bounded `prune` or terminal `remove` for intentional
cleanup. Attempts are deleted with jobs. Removing history also releases stable-ID deduplication, so
choose retention longer than the application's reconciliation window. Error messages are sanitized
and capped at 4000 characters, without stacks or payloads; avoid throwing secrets.

Pool waits spend statement and lease budgets. Size the shared pool for actual handler/application
SQL occupancy plus queue operations; handler concurrency is not a connection count. A small saturated
pool can cancel an attempt even when the application process remains healthy.

## Schema and validation limits

Setup explicitly installs one reserved `pgclear_jobs` schema. Workers only check its compatibility;
producers never perform DDL. A deliberate down migration removes the namespace and its dependants.
An explicit down/up reset may lose queued jobs and history. See [schema setup](migrations.md).

The test suite covers real PostgreSQL rollback, competing workers, process death, lock waits,
follow-up promotion, schedule arbitration, retention, and shared-pool pressure. A test-only TCP relay
exercises delayed/lost requests and responses, uncertain commits, stale tokens, and lost rollback
responses. Synthetic load profiles exercise backlogs alongside application traffic.

Those checks do not establish deployment capacity or validate every application effect, TLS topology,
database failover, or long-term production behavior. Validate the target application's idempotency,
cancellation, supervision, and rollout. See [development](development.md) for reproduction commands.

Completion/failure events require acknowledged database outcomes and do not replace durable
job/application state.
Handler-error decisions follow confirmed managed rollback; uncertain transaction outcomes still
reach supervision. See [observation and error policy](api.md#observation-and-error-policy).
