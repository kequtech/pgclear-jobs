# Storage and reliability

## Ownership and lifecycle

The app owns persistence. `installJobs` creates/upgrades the reserved `tenon_jobs` schema in one
transaction, serialized by an advisory transaction lock. Version 4 adds manual retry links, adapts
occurrence uniqueness and indexes terminal cleanup; earlier job data and saved policies remain intact.
Unknown newer schema versions are rejected. Producers never install storage; runners check compatibility.
Do not manually add or modify objects in this reserved schema.

```text
pending → running → completed
             ├──→ pending   (retry with backoff)
             └──→ failed    (permanent failure or exhausted budget)
pending → cancelled
failed ── manual retry ──→ new pending record linked to the retained failure
```

All SQL uses bound parameters through a narrow execution boundary. `jobs(tx)` participates in the
caller's transaction without owning it. Commits expose jobs and notifications together; rollbacks
expose neither. A facade retains its scope and obeys its lifetime. Keep app transactions short.

## Submission identities and updates

A job UUID identifies a retained submission across every state. Reusing an explicit ID returns the
original record; deleting it releases that deduplication. An outstanding key instead identifies one
pending/running record per definition. Terminal states release keys. Neither contract guarantees
exactly-once effects after execution begins.

Database uniqueness arbitrates collisions. Single submission uses a no-op conflict update to return
the current record even under contention. A pending upsert replaces payload and any supplied timing
on the locked row, checking its current state; it reports running work as an application conflict.
Bulk insertion skips conflicting records and reports only inserted jobs. Manual retry inserts a fresh
linked record, checks that the source is failed, and refuses an occupied outstanding key without a
unique-constraint error that would abort the app transaction.

No automatic SQL retry hides uncertain commit outcomes. Reconcile lost responses with retained
submission IDs or application state. Repeating unkeyed submission or manual retry can create more work.
Read-then-write logic cannot guarantee a trailing execution when an upsert collides with active work.

## Claims, budgets and cancellation

Claims select due pending jobs using `FOR UPDATE SKIP LOCKED`, update state with a fresh token and
insert attempt history in one statement. No row lock or transaction spans a handler. Unknown names
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
and a Node warning uses code `TENON_JOBS_NOTIFICATIONS_UNAVAILABLE`. LISTEN needs a dedicated direct
or session-pooled connection; a query pool may use transaction pooling.

Schedules are durable producers, not a second executor. A pass locks at most 50 due schedules for its
selected names, submits ordinary jobs and advances cursors in one transaction. Occurrence uniqueness
prevents duplicate original submissions. Explicit manual retries are separate linked attempts at the
same occurrence and retain overlap protection. Removing a producer leaves its jobs intact.

The default missed policy submits the saved due occurrence once and advances directly into the
future. It never walks every missed tick. Skip mode drops a cursor more than one minute late.
Default overlap suppression counts pending/running work, including automatic and manual retries.
Calendar/DST calculation follows cron-parser. Using the saved forward cursor avoids changing an
occurrence's identity through a reverse lookup around clock changes. No active matching runner means
no recurring submissions; processing resumes when one starts.

## Shutdown and operations

Abort stops new claims and maintenance. An in-flight claim received after stop is left for recovery.
Active handlers get the shutdown grace to finish; then signals abort and renewal stops. In-flight
database operations can add up to their timeout. Uncooperative handlers may outlive the runner's wait;
process isolation/supervision remains app-owned. A database fault rejects the runner, signals active
work and leaves unfinished claims for recovery. Borrowed pools stay open; owned pools/listeners close.

Terminal retention is explicit. `remove` and bounded `prune` delete history with jobs; failed records
are preserved by default. Precise keyset cursors survive deleting earlier rows, but pagination is not
a snapshot across calls. Statistics read current retained records without persistent counters.
Schema installation, transaction rollback, competing runners, process death, lock waits, delayed
responses, notifications, reconnects, schedule contention/DST, cleanup and upgrades have behavioral
tests on PostgreSQL. Outstanding validation work is tracked only in [todo.md](../todo.md).
