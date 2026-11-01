# Storage and reliability

## Ownership and lifecycle

The app owns persistence. `pgclear-jobs migrate` installs the reserved `pgclear_jobs` schema in one
transaction, serialized by an advisory transaction lock. Repeated installation leaves current storage
intact. `pgclear-jobs migrate down` intentionally removes the entire namespace and all Jobs data.
Stop producers and workers before removal. A breaking schema change can use an explicit down/up reset;
there is no incremental migration engine. Application migration tools can extract the same up/down SQL.
An internal schema marker lets workers reject incompatible storage. Producers never install storage;
runners only check compatibility at startup. Do not add application objects to this reserved schema.

```text
pending → running → completed
             ├──→ pending   (retry with backoff)
             └──→ failed    (permanent failure or exhausted budget)
pending → cancelled
blocked pending follow-up ── predecessor terminal ──→ eligible pending
failed ── manual retry ──→ new pending record linked to the retained failure
```

All SQL uses bound parameters through a narrow execution boundary. `jobs(tx)` participates in the
caller's transaction without owning it. Commits expose jobs and notifications together; rollbacks
expose neither. A facade retains its scope and obeys its lifetime. Keep app transactions short.

Definitions may opt into `transactional: true`. Claims still commit separately before execution.
The runner opens a pgclear transaction and supplies it as `tx`; an integration can supply its own
pgclear-backed transaction API. Application writes, follow-up submissions, successful attempt history and completion commit
together. A failed completion fence or handler exception rolls back application writes; handler
failure/retry bookkeeping is recorded afterward using the ordinary fenced statement. Native driver
cancellation expires and rolls back the transaction even when JavaScript ignores the abort signal,
while the unfinished handler retains its local slot. Heartbeats use separate pooled connections and
the job row is locked only during completion, not throughout the handler. These transactions hold a
connection and application locks until commit/rollback, so keep them short and leave pool capacity
for renewals and maintenance. Database/commit failures reject the runner without replaying the
transaction; an uncertain commit includes both application effects and completion, or neither.
Only operations through the supplied transaction participate in this guarantee.
Expected cancellation is suppressed only when driver cleanup succeeds. Lost rollback responses and
connection-release failures reject the runner even during shutdown, preserving aggregate driver
errors and the handler failure's sanitized cause for supervision.

## Submission identities and updates

A job UUID identifies a retained submission across every state. Reusing an explicit ID returns the
original record; deleting it releases that deduplication. An outstanding key identifies one unblocked
pending/running head per definition, optionally with one blocked pending follow-up created by
`request`. Terminal transitions release the key or transfer it atomically to that successor.
Neither contract guarantees exactly-once effects after execution begins.

Database uniqueness arbitrates collisions. Stable-ID submission uses a no-op conflict update to
return the original record even under contention. Independent `send` and `sendMany` jobs have no key;
only `request` coalesces keyed work. Bulk insertion skips duplicate IDs and reports only inserted jobs. Manual retry inserts a fresh
linked record, checks that the source is failed, and refuses an occupied outstanding key without a
unique-constraint error that would abort the app transaction.

No automatic SQL retry hides uncertain commit outcomes. Reconcile lost responses with retained
submission IDs or application state. Repeating unkeyed submission or manual retry can create more work.

`request` uses an installed PostgreSQL function to lock or create the outstanding head. Unstarted
heads can be replaced. Once an attempt has started, requests preserve the head, including during
retry backoff, and coalesce into a separately indexed successor. Fresh statement snapshots handle
completion or cancellation during conflict lock waits. No transaction is opened behind the caller's
scope and no application-supplied callback runs under these locks.

A terminal-transition trigger promotes the pending successor in the same transaction as completion,
permanent failure, exhausted recovery or pending cancellation. It clears the blocker only after the
old head releases its unique outstanding-key slot. Claims and next-due timers exclude blocked rows.
Promotion emits a transactional notification even when payload and requested time stay unchanged.
Rollback restores both predecessor and successor, and terminal cleanup cannot strand pending work.
Automatic retries retain the blocker and original payload; successors get their own IDs, saved
policies, attempt budgets and history. This guarantees coalesced trailing work, not one execution
per request. Concurrent requests use database serialization order rather than producer timestamps.

## Transport failures and reconciliation

A rejected submission or transaction is not proof that PostgreSQL rolled it back. The server may
have committed while its reply was lost. Errors propagate from pgclear without transaction replay;
an uncertain managed COMMIT reports `PGCLEAR_COMMIT_UNKNOWN`. The queue does not infer success or
failure from a disconnected socket or wrap that uncertainty in another recovery API.

For a submission that must be reconciled, choose its UUID once, before attempting the write, and
retain it outside the uncertain transaction. `get(id)` finds the retained record and repeating
`send(def, originalPayload, { id })` returns it unchanged, even after completion. Stable-ID protection
lasts only while the record is retained. It does not make external handler effects exactly once.

For application writes and a job submitted through `jobs(tx)`, reconcile application state as well
as the job. A missing row while the original transaction is still alive is inconclusive. Wait until
that transaction has a definitive outcome before treating absence as a rollback, or use app-owned
unique request identities that arbitrate a concurrent retry. Do not blindly rerun business writes
because job submission rejected. Outstanding keys and `request` coalescing are not stable submission
identities: a repeated request after its previous work terminates can create another run.

Workers stop trusting ownership at their last locally confirmed lease deadline. A renewal received
by PostgreSQL but whose reply is held in transit cannot extend that local budget. The handler's
signal aborts independently of a longer database-operation timeout. On transport recovery, late
claim responses cannot start expired work and stale renewal/completion tokens cannot regain ownership.
If completion committed but its response was lost, the runner rejects; a supervised replacement
observes the terminal record and does not execute that completed job again.

These boundaries have integration tests through a test-only plaintext TCP relay against PostgreSQL:
held requests, held responses, disconnected sockets, both outcomes of an uncertain COMMIT through
native and integrated query scopes, stable-ID reconciliation, lease cancellation, stale tokens and lost completion
responses. A separate healthy connection observes real commit/rollback and lease state. The tests
add no runtime dependency or public API. They do not simulate TLS negotiation failures, database
failover or kernel-level packet retransmission; those remain deployment-specific checks.

## Claims, budgets and cancellation

Claims select due pending jobs using `FOR UPDATE SKIP LOCKED`, update state with a fresh token and
insert attempt history in one statement. Each runner rotates the preferred definition after every
successful claim. Indexed per-definition lookups inspect at most one due candidate per definition,
then select the first eligible turn. Within each definition, due time and UUID order remain intact.
Next-due calculation likewise takes only the earliest future candidate per definition. This avoids
sorting a whole backlog to claim one job and preserves the shared snapshot for claims and timers.
This is local admission fairness, not preemption or a global weighted scheduler. No claim transaction
spans a handler; opt-in handler transactions hold application locks. Unknown names
are left alone. Every outcome and heartbeat checks the current token and live lease. Successful
completion also checks the execution deadline. These time checks run **after** acquiring the row
lock: a lock-only wait must not authorize an operation using an earlier time check.

Heartbeats normally run every third of the lease, bounded by the attempt deadline. Claim and renewal
round trips count against conservative local budgets using a monotonic clock. A response that has
spent its budget cannot start a handler or extend locally assumed ownership. A local timer cancels
work if renewal confirmation is late. Pool waits can consume an attempt without running its handler;
allow adequate lease time for database latency.

Automatic retries share one policy for handler errors and expired claims. Exponential delay is capped
before optional jitter. `JobError(message, { retry: false })` fails immediately; `retryAt` provides a minimum
next-attempt time, taking the later of provider timing and policy backoff. Neither extends the budget.
Error text is capped at 4000 characters, with invalid text replaced, and contains no captured stack
or payload. Apps must keep secrets out of thrown messages.

Recovery locks at most 100 expired claims per maintenance pass, updates their state and history
atomically, and serializes with completion/renewal. It recovers all definition names. A runner losing
ownership stops recording outcomes and signals cooperative cancellation. Cancellation cannot stop
arbitrary JavaScript, blocked event loops, or completed external effects.

Unfinished handlers retain their local slot after timeout or lost ownership until they settle or
shutdown spends its grace. Another process can recover and retry their claim. Local concurrency is
not distributed exclusivity. Execution remains at least once; external effects need app-level
idempotency or reconciliation even when the queue completion is fenced correctly.

## Discovery and schedules

A transactional trigger emits definition names on pending insertion, state transition or timing change.
This includes future jobs, allowing runners to adjust their timers. Payloads are never broadcast.
Generation-counted wakeups retain notifications received between checking storage and going idle.
The claim and next-due calculation share a snapshot, closing a separate-query timing gap.

With notifications, fallback reconciliation defaults to 30 seconds. Without them it defaults to one
second. Initial listener failures retry each second; pgclear reconnects an established listener.
Reconnection triggers reconciliation. While a listener is unavailable, fallback is capped at one second
and a Node warning uses code `PGCLEAR_JOBS_NOTIFICATIONS_UNAVAILABLE`. LISTEN needs a dedicated direct
or session-pooled connection; a query pool may use transaction pooling.

Schedules are durable producers, not a second executor. A pass locks at most 50 due schedules for its
selected names, submits ordinary jobs and advances cursors in one transaction. Occurrence uniqueness
prevents duplicate original submissions. Explicit manual retries are separate linked attempts at the
same occurrence. A unique partial index permits only one pending/running job per schedule, so a
manual retry of an older failure raises `JobStateError` while a later occurrence is outstanding.
Concurrent scheduler/retry inserts share that arbitration: a losing scheduler skips the occurrence;
a losing manual retry raises `JobStateError`. Removing a producer leaves its jobs intact.

A schedule submits its saved due occurrence once and advances directly into the future. It never
walks every missed tick. Overlap suppression counts pending/running work, including automatic and
manual retries. Catch-up and overlap behavior are fixed; there is no policy matrix to configure.
Calendar/DST calculation follows cron-parser. Using the saved forward cursor avoids changing an
occurrence's identity through a reverse lookup around clock changes. No active matching runner means
no recurring submissions; processing resumes when one starts.

## Shutdown and operations

Abort stops new claims and maintenance. An in-flight claim received after stop is left for recovery.
Active handlers get the shutdown grace to finish; then signals abort and renewal stops. In-flight
database operations can add their remaining timeout and driver cancellation/cleanup time.
Uncooperative handlers may outlive the runner's wait;
process isolation/supervision remains app-owned. A database fault rejects the runner, signals active
work and leaves unfinished claims for recovery. The application pool stays open; the optional
runner-owned listener closes.

Runners retain completed/cancelled history for seven days, then remove it in batches of at most 100
using a single database statement-time cutoff and skipping locked rows. Per-definition index scans
use a completed/cancelled-only index, excluding retained failures, and consider at most 100 expired
candidates each, then one global batch deletes at most 100. Cleanup
runs for selected definitions in the existing maintenance loop, checks once a minute, and drains a full batch on the next maintenance
pass. No outstanding or failed work is expired. Runner `retentionMs` extends this window or disables
cleanup with `false`; runners sharing definitions should agree on retention. Failed records require
explicit operator cleanup. `remove` and bounded `prune` also delete attempt history with jobs.
Deleting a stable-ID job releases its deduplication; retain history longer than the application's
reconciliation window. Precise keyset cursors survive deleting earlier rows, but pagination is not
a snapshot across calls. Statistics read current retained records without persistent counters.
Schema installation, transaction rollback, competing runners, process death, lock waits, delayed
responses, notifications, reconnects, schedule contention/DST, cleanup, fresh installation, keyed
follow-ups, automatic retention, backlog fairness, shared-pool contention and transport failures
have behavioral tests on PostgreSQL.
Outstanding validation work is tracked only in [todo.md](../todo.md).

## Load validation

Measurements on 1 November 2026 used local PostgreSQL 16.15 and Node 24.4.1, with two runners in
one Node process, four definitions, and a shared query pool of four or two connections. Each profile
began with 1,500 ready bulk jobs, 20,000 retained terminal records and 3,000 expired records. Producer
batches added bulk, quiet, retrying and 200 ms delayed work while two simulated application clients
used the same pool. The contended profile also held handler/application connections for 8/12 ms.

The comparison offered 11 jobs every 50 ms for 10 seconds, then drained outstanding work. Actual
submission totals differ because shared-pool pressure also applies backpressure to producers.

| Measurement                                                | Four connections: before → after | Two connections: before → after |
| ---------------------------------------------------------- | -------------------------------- | ------------------------------- |
| Quiet definition p95 start delay                           | 9702 → 18 ms                     | 38069 → 1204 ms                 |
| Enqueue p95                                                | 34.78 → 18.48 ms                 | 71.22 → 39.37 ms                |
| Application transaction p95                                | 17.08 → 7.62 ms                  | 52.18 → 40.55 ms                |
| Completed jobs/second, including initial backlog and drain | 191 → 326                        | 68 → 83                         |
| Failed jobs / expired attempts                             | 0 / 0 → 0 / 0                    | 0 / 0 → 0 / 0                   |

Before the change, claim plans sorted the entire matching ready backlog and automatic cleanup used
a sequential scan plus a sort over expired matches. Afterward, plans used existing `jobs_pending`
and `jobs_terminal` indexes with bounded per-definition scans. No new indexes, schema changes,
claim batches, priorities, queue policies or public options were introduced for these improvements.
The deterministic regression first failed on the original dispatcher, then passed with rotation.

A separate steady-load run offered 11 jobs every 150 ms for **60 seconds per profile**, followed by
backlog drain. Each profile completed 5,922 jobs, including the 1,500 initial jobs, with exactly the
expected retries, zero expired attempts, zero failures, and all 3,000 expired history records removed.
The quiet definition's p95 start delay was 18.85 ms with four connections and 44.23 ms with two.
The contended bulk backlog took longer to drain; fair admission does not create processing capacity.

These are single-host synthetic measurements, not a throughput or latency guarantee for applications.
DB sleep models connection occupancy, not real business work. Network RTT, payload/transaction size,
worker process layout and concurrent app traffic will change results. Pool peaks are sampled every
10 ms; plan timings are single `EXPLAIN ANALYZE` observations. This is short sustained validation,
not evidence of long-term production operation. The [recorded results](load-results.json) retain
configuration, timings, sampled peaks and plan summaries.

Reproduce against a dedicated database ending in `_test`:

```sh
npm run test:load
PGCLEAR_JOBS_LOAD_DURATION_MS=60000 PGCLEAR_JOBS_LOAD_INTERVAL_MS=150 npm run test:load
```

The default phase is 10 seconds with a 50 ms interval; each batch contains 11 jobs. Set
`PGCLEAR_JOBS_LOAD_OUTPUT` to save JSON. Setup, drain and cleanup add time beyond the production phase.
Only the generated load-test definitions are deleted afterward. `npm test` runs smaller behavioral
regressions for fairness, oldest-due ordering, lock skipping, shared-pool load and total pool exhaustion.

## Before app adoption

Review the current [public API](api.md), not the earlier pending-upsert prototype. Ordinary event
handlers need domain idempotency; keyed producers need application-owned arbitration for forced
requests and generated results. Retain stable IDs longer than their reconciliation window.
Use per-definition limits or separate workers for long/short latency isolation: fair admission does
not preempt work already occupying every local slot. Pool sizing must cover actual SQL occupancy,
not just the number of handlers, and runner failure/shutdown must join the app's existing supervision.

A real-app maintenance canary and one-workflow-at-a-time cutover remain the adoption checks. Stop
old producers/schedulers and drain or explicitly transfer delayed/retrying/active old work before
both systems can execute the same workflow. App payload compatibility must be considered across
versions. A severe schema change may use an explicit down/up reset, intentionally losing queued
work and history. Saved application migration files remain immutable. Database failover,
TLS/network topology and app effects remain checks for the target deployment.
