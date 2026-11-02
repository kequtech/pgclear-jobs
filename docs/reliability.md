# Reliability and application boundaries

[Overview](../README.md) · [Reference](reference.md) · [Hosting](hosting.md)

## Submissions and ownership

jobs(pool) uses independent pool operations; jobs(tx) joins the supplied transaction. Committing
application state and submission together also commits notification visibility. Rollback removes
both. Stable job IDs deduplicate only while their records remain; retention/removal releases them.
Payload types are declarations, not runtime validation of persisted data.

A claim commits before handler invocation and carries a fresh ownership token, deadline and lease.
Heartbeats renew it. Completion/history updates require the current running token. Expired claims
are recovered in bounded maintenance passes, independently of available handler slots. Lost claims
and aborted scopes cannot later acknowledge stale completion.

## Choose a transaction boundary

A transactional handler receives a real supplied tx. Application writes, follow-up submissions,
success history and completion commit together when performed through that scope. Handler errors
roll back before retry bookkeeping; refused completion rolls back too. Calls through other pools
or independently opened transactions are outside that boundary.

Use short transactional handlers for database work. Keep remote generation, email and object storage
outside long transactions; the transaction otherwise holds a pool connection and locks throughout.
A nontransactional handler can prepare results and persist in short independent transactions, but
those can commit before queue acknowledgement. A retry must recognize already committed results.

Persist stable business operation IDs, result/checkpoint references and required follow-up obligations
in application tables. A result alone does not prove a separately committed follow-up exists. Put
them in the same short transaction where possible. Business results must survive queue pruning.
A failure status written inside a throwing transactional handler rolls back with it.

## Retries and uncertain outcomes

Ordinary handler errors follow saved retry policy. maxAttempts includes the first invocation. Retry
timing uses the database clock, backoff/cap/jitter and an optional JobError.retryAt lower bound.
retry:false marks permanent failure. Changing definitions does not rewrite existing saved policy.

Opening, committing or cleaning up a Jobs-managed transaction fails the runner on infrastructure
failure, preserving original driver/aggregate errors. Independent transaction failures inside a
handler normally remain handler failures, so they may be retried; use onHandlerError to escalate
when the application's reconciliation policy requires it. See [hosting](hosting.md#observe-and-escalate).

A rejected commit does not prove rollback. pgclear's [outcome guide](https://github.com/kequtech/pgclear/blob/17fae30540cbc41ce2c776645437c6d265e2de96/docs/errors.md)
and findDriverErrors expose operation evidence without guessing whole-transaction success.
Reconcile uncertain effects through authoritative storage before replay or destructive compensation.
Keep stable IDs/storage keys outside the uncertain transaction. An uploaded object may already be
referenced by a committed row; retain it for orphan cleanup if reconciliation is unavailable.
Provider idempotency keys help, but database fencing cannot make external effects exactly once.

## Keys and schedules

A request key permits one active head and one coalesced successor. Requests replace unstarted
payloads; after an attempt starts they retain the head through all retries. Terminal transition
promotes the successor in that same transaction. Payloads replace rather than merge; explicitly
supplied timing replaces timing, while omitted timing preserves it. Sticky force/debounce intent
belongs in application state serialized with submission.

Ordinary schedules suppress overlapping work from that schedule, including manual retries. A keyed
schedule instead invokes the same request function as manual/startup triggers and preserves existing
due times/backoff. Its requestKey is distinct from its schedule identity key. Shared jobs have no
exclusive schedule ID/date; each schedule records lastJobId. Removing/changing a schedule affects
future production, not already submitted jobs. See [schedule details](reference.md#recurring-schedules).

A key orders valid claims. An expired handler may still execute JavaScript or an external effect.
Application operations must remain safe under repetition.

## Cancel a job

```ts
const job = await jobs(pool).cancel(jobId);
// Pending work is cancelled. Running work has cancelRequestedAt set.
```

The same call works from any process using the database. Pending work becomes cancelled immediately.
For running work, the request is durable; the owner detects it on its next heartbeat and aborts the
handler's existing signal. Detection normally takes up to leaseMs / 3 (10 seconds by default), plus
database waits. Notifications are not required. Pass the handler signal to cooperative dependencies:

```ts
async handler(payload, { signal }) {
  const response = await fetch(payload.url, { signal });
  signal.throwIfAborted();
  // Persist results only while the application operation is still valid.
}
```

cancel returns after recording intent, not after the handler stops. Inspect state with get/getMany/find
when needed. Repeated cancellation is safe. A completed/failed job rejects with JobStateError; a
missing ID rejects with JobNotFoundError. Cancellation inside jobs(tx) becomes visible on commit.
To cancel the current job from its handler, use the root jobs(pool) facade: a request made through
that handler's managed transaction rolls back when it prevents the job's completion.

Managed handler writes roll back when cancellation wins completion. Cancellation does not retry:
the worker acknowledges it after callback cleanup, or lease recovery marks it cancelled if the worker
is lost. Attempt history records cancelled. A completion committed before cancellation wins remains
completed. Cancellation preserves lastError from an earlier failed attempt; the cancelled attempt's
error is null. Terminal transition promotes a keyed successor; cancellation targets only this job, so
cancel the successor separately if needed. Schedules continue producing until unscheduled.

An uncooperative handler retains its local concurrency slot until it settles or runner shutdown
abandons it. Lease recovery may already have made the record terminal while that JavaScript still
runs. Cancelled is queue state, not proof that an external effect stopped or was undone. Keep business
fences, provider idempotency and reconciliation where stale/external results matter.

## Capacity, progress and notifications

Concurrency limits are per runner, including per-definition limits. Multiple processes do not
share a global concurrency cap. Claims rotate between eligible definitions so an old backlog does
not monopolize local admission. Reserve capacity for short work with definition limits/separate runs.

A timed-out/uncooperative handler keeps its slot until it settles or runner shutdown spends its grace.
A saturated pool can prevent timely renewal even while the process is otherwise alive. Size the pool
for application work, transactional handlers and queue maintenance; concurrency is not a connection
count. Continuous progress monitoring/restarts remain application responsibilities.

Optional notifications use a separate pgclear listener with polling fallback. Failed listeners warn;
reconnect/gaps trigger authoritative queue checks. Notifications are wakeups, not durable work.
Their connection requires direct/session pooling and sits outside the query pool. pgclear owns
registration/reconnect behavior; Jobs owns queue reconciliation.

## Inspect and retain

find is live cursor pagination, default 100, max 1000. Continue with the last record's cursor:

```ts
let after: string | undefined;
for (;;) {
  const page = await jobs(pool).find(spec, { limit: 100, ...(after ? { after } : {}) });
  if (!page.length) break;
  for (const job of page) await project(job); // Application projection.
  after = page.at(-1)!.cursor;
}
```

Cursors preserve database timestamp precision and work after cursor-row removal. getMany reads up
to 1000 heterogeneous IDs once each in requested order, omitting missing rows. Queue records expose
operational state; durable product status belongs in application records.

Automatic retention covers completed/cancelled records, default seven days; failures/outstanding
work remain. Pruning is bounded, skips locked rows and cascades attempts. Shared runners should agree
on retention because the shortest policy can remove shared history. Retain deduplication/history for
the reconciliation window. Persisted diagnostics are sanitized and capped at 4000 characters, not
stacks/payloads; keep secrets out of thrown messages.
