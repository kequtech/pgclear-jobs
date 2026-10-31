# Storage and reliability

## Model before table count

One shared set of tables per database is agreed. Exact DDL, table count, names, and indexes are not.
`tenon_jobs` was suggested as a schema name, not finalized.

A persisted job needs a stable ID, definition name, payload, execution state, earliest execution
time, attempt bookkeeping, current claim ownership, and lifecycle timestamps. The working data
types are UUID IDs, JSON payloads, and time-zone-aware timestamps; exact contracts remain open.

Attempt history, schedules, worker activity, and migration metadata may justify additional tables.
Earlier suggestions of three or five tables are not requirements. Decide each table from an agreed
responsibility, not from a target count or a desire to reproduce pg-boss.

## Working lifecycle

```text
pending → running → completed
             │
             ├→ pending  (retry with a later execution time)
             └→ failed   (attempt budget exhausted)

pending → cancelled
```

Future jobs can remain pending with a future execution time. Retrying jobs can return to pending;
attempt information distinguishes them from jobs that have never run. Separate scheduled and
retry states are not currently necessary. This lifecycle is a working proposal, including the
pending-only cancellation rule.

Behavior is defined in application code. Whether retry/timeout policy is snapshotted at submission,
resolved at execution, or versioned remains an important open decision: deployments must have
predictable effects on existing jobs. Local concurrency remains a runner concern.

## Outstanding-work keys

Lookup and duplicate prevention are both important, distinct capabilities. Reading existing work
cannot guarantee that a concurrent caller will not insert another job.

The proposed simple contract for optional `send(..., { key })` is:

- Scope a key to a definition's stable name.
- Permit at most one outstanding record for that name and key.
- Pending and running records are outstanding, including retries waiting to run.
- Return an existing outstanding record without changing its payload or execution time.
- Completed, permanently failed, and cancelled records release the key.
- With no key, submissions are independent records.

`send` returns the record directly, with no creation flag. Record state cannot establish whether a
particular submission inserted it. The key lifetime and return behavior above remain a working
proposal, rather than a fully accepted deduplication specification.

Uniqueness must be database-enforced, potentially with a partial unique index. The implementation
must account for concurrent inserts, rows changing state, and transaction isolation when returning
the existing record. A naive insert-or-select statement may not establish all promised semantics.

This key prevents duplicate outstanding work; it is not permanent submission idempotency and does
not prevent repeated external effects. A job may finish between an uncertain commit and a retry,
releasing its key. Stable submission IDs or a separate idempotency contract need consideration for
uncertain enqueue outcomes. Manual retry must also respect any currently occupied key.

The same key can support application lookup. A separate non-unique application-reference feature
has not been selected.

## Concurrent record mutations

Eligibility must be checked in the write itself. A runner may claim a record between an application
read and a reschedule, payload update, or cancellation. Mutations must report that conflict rather
than silently modifying active work or pretending to cancel effects already underway.

No operation should take ownership of, commit, or roll back the caller's transaction. Error types,
missing-record behavior, stale-read/version checks, and active cancellation remain to be specified.
When reporting an error inside a caller transaction, distinguish a deliberate application conflict
from an SQL error that has aborted that transaction.

## Claim and recovery requirements

- Claim atomically across runners, using short operations such as `FOR UPDATE SKIP LOCKED` plus a
  state transition. Never hold a row lock for the duration of a handler.
- Assign every attempt a fresh claim token. Completion, failure, and renewal must check the current
  token and applicable state so a stale runner cannot modify a newer attempt.
- Give claims an expiry and recover abandoned attempts in bounded batches. Recovery itself must
  coordinate safely with completion and renewal.
- Distinguish lease renewal from a maximum execution deadline. A healthy heartbeat must not extend
  an attempt indefinitely beyond its execution policy.
- Treat PostgreSQL rows as authoritative. Notifications reduce discovery latency; polling recovers
  missed notifications and discovers delayed work.
- Bound retries and preserve exhausted work for inspection. Do not silently delete outstanding or
  failed work through an implicit retention policy.
- Keep cleanup and recovery transactions short, with suitable indexes and bounded batches.
- Define shutdown for running and uncooperative handlers. A timeout or lost lease cannot itself
  force arbitrary JavaScript or an external operation to stop.

Execution is at least once. An external action can succeed before completion is recorded, and a
crash can cause another attempt. Claim fencing protects queue records, not external side effects.
Handlers need application-appropriate idempotency. A special handler transaction API is not the
solution chosen for this library.

## Inspection and operations

Applications need bounded lookup, job state, scheduling information, attempts, and useful failure
details. Operations need to identify overdue work, growing backlogs, failures, and absent or
incompatible runners. Persistence alone does not establish that work is being processed.

Derive counts, ages, and durations from authoritative records before adding stored counters or a
historical metrics subsystem. A worker registry may improve visibility, but it is not required as
the authority for claim recovery. Error serialization, sensitive-data handling, history retention,
and explicit cleanup controls still need a concrete contract.

## Validation before relying on the library

Validation should exercise observable behavior: transaction commit/rollback, concurrent claims,
concurrent keyed submissions, delayed execution, mutation/claim races, bounded retries, stale
completion, recovery after process loss, and shutdown. Include real PostgreSQL integration tests.

Before production trust, also investigate network failures and uncertain commits, sustained load,
database maintenance, schema upgrades, and mixed application versions. Do not substitute tests
that inspect documentation wording for behavior tests.

Reference mechanisms: [PostgreSQL locking clauses](https://www.postgresql.org/docs/18/sql-select.html#SQL-FOR-UPDATE-SHARE),
[partial unique indexes](https://www.postgresql.org/docs/18/indexes-partial.html), and
[pg-boss 12.38.0 schema and SQL](https://github.com/timgit/pg-boss/blob/12.38.0/src/plans.ts).
