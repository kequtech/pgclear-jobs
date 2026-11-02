# Reference

[Overview](../README.md) · [Storage](storage.md) · [Reliability](reliability.md) · [Hosting](hosting.md)

The package root exports definitions, record operations, runners, setup functions and error types.
`/migrations` exports concrete SQL; `/integration` binds another pgclear-based query API.

## Producer contracts and workers

```ts
const spec = defineJob<{ id: string }>({ name: 'refresh', maxAttempts: 3 });
const worker = defineWorker(spec, {
  transactional: true,
  async handler(payload, { tx }) {
    await tx.queryClassic('SELECT $1 AS id', [payload.id]);
  },
});
```

Import defineJob/defineWorker from pgclear-jobs. JobSpec is immutable handler-free submission metadata;
JobDefinition is executable. WorkerOptions accepts handler, transactional and concurrency, retaining
inferred payload/native pgclear transaction types. Worker bindings cannot change saved policy/name.
An inline defineJob({name,handler,...policy}) remains supported. Runners require executable uniquely
named definitions. Validation happens before pool access.

| Saved policy option | Default / bounds                          |
| ------------------- | ----------------------------------------- |
| name                | Required nonblank name; 1–200 characters  |
| maxAttempts         | 3 total attempts, including first; 1–1000 |
| retryDelayMs        | 1000; 0–86400000                          |
| retryBackoff        | exponential or fixed; default exponential |
| retryMaxDelayMs     | 86400000; zero allowed                    |
| retryJitter         | true; half-to-full capped delay           |
| executionTimeoutMs  | 300000 per attempt                        |

transactional defaults false. Definition concurrency defaults to the runner's limit, range 1–1000;
it is local per runner. Handler/transactional/concurrency are runtime settings, not saved job policy.
Context has id, one-based attempt, saved maxAttempts, signal and scheduledFor:Date|null. Transactional
handlers additionally receive tx. Handlers return void/Promise<void>; business results belong in
application records. Payloads are strict JSON primitives/dense arrays/plain objects. Undefined object
fields omit; top-level/array undefined, Date, bigint, nonfinite numbers, cycles/accessors and invalid
Unicode/NUL reject. TypeScript types do not validate persisted payloads.

## Record scope and submissions

`jobs(pool)` executes through that pool; `jobs(tx)` joins that transaction and expires with it. The
facade opens no hidden transaction. All operations below use that scope. The adapter equivalent is
[documented separately](integration.md).

| Method                                      | Result / behavior                                                                                                       |
| ------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| send(spec,payload,{id?,runAt?,delayMs?}?)   | Independent Job<Payload>; stable ID returns its original unchanged record while retained                                |
| sendMany(spec,submissions)                  | Up to 1000 {payload,id?,runAt?,delayMs?}; insert once, skip duplicate IDs, return inserted jobs without order guarantee |
| request(spec,payload,{key,runAt?,delayMs?}) | Replace an unstarted head or create/update one coalesced successor behind a started head and its retries                |

Timing accepts runAt:Date or delayMs, never both. Relative delay uses the PostgreSQL clock; new work
with omitted timing is immediately eligible. Existing request timing is preserved unless explicitly
replaced. Keys are scoped to the definition and accept up to 500 characters, including empty.
Terminal completion/failure/cancellation releases a blocked successor atomically. Payloads replace,
not merge. Empty sendMany returns [] without SQL. Unknown definition/submission/schedule/runner
options reject instead of silently changing behavior.

## Inspection and mutations

| Method                                   | Result / behavior                                                                                   |
| ---------------------------------------- | --------------------------------------------------------------------------------------------------- |
| get(id)                                  | Job<unknown> or null                                                                                |
| getMany(ids)                             | Up to 1000 UUIDs in one statement; found records once in first-requested order, missing IDs omitted |
| find(spec,{key?,states?,limit?,after?}?) | Typed jobs in creation-time/UUID order; default 100, max 1000                                       |
| stats(spec)                              | State counts plus total, ready, delayed, blocked                                                    |
| attempts(id,{limit?}?)                   | Latest first; default 100, max 1000; missing returns []                                             |
| update(spec,id,payload)                  | Replace pending payload of the selected definition                                                  |
| reschedule(id,runAt)                     | Set a pending execution time                                                                        |
| cancel(id)                               | Cancel pending work or request running cancellation; repeated calls are safe                        |
| retry(id)                                | Fresh linked job from a failed source with copied policy/payload/key/occurrence and fresh budget    |
| remove(id)                               | Delete terminal record and attempts; false if absent                                                |
| prune(spec,{before,states?,limit?})      | Bounded terminal cleanup; default completed/cancelled and limit 100, max 1000                       |

UUIDs normalize case. getMany validates the entire batch before SQL; empty input is inert. Payloads
remain unknown for heterogeneous ID reads. find cursors retain database timestamp precision and
work after the cursor row is removed; pagination is live. states:[] matches nothing. Pending edits
preserve policy/history/budget. Retry refuses an occupied key or schedule; unkeyed retries can create
multiple jobs. Missing mutation targets raise JobNotFoundError, ineligible state/name/conflicts raise
JobStateError. Both expose jobId. See [cancellation](reliability.md#cancel-a-job) for signal and completion races. Removal/pruning never delete outstanding work; prune uses
finishedAt < before and skips locked rows. Failed history needs explicit inclusion.

Job fields: id, name, payload, nullable key/blockedBy, state (pending/running/completed/failed/cancelled),
attempts, saved policy, runAt/createdAt/updatedAt, nullable finishedAt/cancelRequestedAt/lastError/scheduleId/scheduledFor/
retryOf, opaque cursor. Attempt fields: number, startedAt, nullable finishedAt/error and outcome
(completed/failed/expired/cancelled/null). Statistics describe retained rows, not lifetime throughput. Pending
includes retry backoff and blocked successors. Related IDs are not guarantees those records remain.

## Recurring schedules

`schedule(spec,payload,{cron,timeZone?,key?,requestKey?})` returns Schedule<Payload>. Cron must be
deterministic five-field syntax, without randomized H. Timezone defaults UTC; key defaults empty.
Schedule identity is (definition,key). Unchanged cron/zone keeps its cursor; a change selects the
next future occurrence. Payload/policy updates affect future jobs. After downtime, one catch-up
submission advances the cursor into the future. Only runners selecting the definition advance it.

Without requestKey, schedules produce ordinary occurrence-owned jobs. One outstanding occurrence
per schedule, including manual retries, suppresses overlap. Original occurrence uniqueness prevents
duplicate submission. Different schedules/manual keyed requests remain independent.

With requestKey, each tick performs the same coalesced request as manual/startup producers, atomically
with cursor advancement. It supplies no timing override: existing head/successor times and retry
backoff remain. Shared jobs have null scheduleId/scheduledFor because they have no exclusive schedule
owner. Each contributing schedule records the affected lastJobId, even when it is an existing job.
Changing requestKey affects future ticks; unscheduling leaves jobs intact. Manual retry uses key
arbitration. Omit requestKey when registering again to restore ordinary production.

Schedule fields: id, name, key, requestKey:string|null, payload, saved policy, cron, timeZone, nextRunAt,
lastJobId:string|null. `schedules(spec,{limit?}?)` lists by key, default 100/max 1000.
`unschedule(spec,key?)` removes the producer. `previewSchedule({cron,timeZone?},{from?,count?}?)`
calculates future Dates strictly after from without a database; defaults now/5, max 100. Cron/DST
calculation follows cron-parser.

## Runner

`runJobs(pool,definitions,options?)` returns the full lifetime Promise<void>.
`startJobs(pool,definitions,options?)` returns RunnerHandle {ready,done}. Both use the same engine,
leave the supplied pool open and support independent runs. A pre-aborted run acquires nothing.
[Hosting](hosting.md) owns readiness settlement, signal ordering and process examples.

| Runner option      | Default / contract                                                            |
| ------------------ | ----------------------------------------------------------------------------- |
| signal             | Application-owned shutdown signal                                             |
| onReady            | Optional awaited callback after schema validation, before dispatch            |
| concurrency        | 1 unfinished handler; 1–1000, including cancelled callbacks until they settle |
| notifications      | Optional dedicated listener URI/ConnectionOptions; omitted means polling      |
| pollIntervalMs     | 1000, or 30000 with notifications                                             |
| recoveryIntervalMs | 1000; maintenance independent of handler slots                                |
| leaseMs            | 30000; minimum 30; heartbeat every third                                      |
| shutdownTimeoutMs  | 30000; zero allowed                                                           |
| databaseTimeoutMs  | 10000; queue SQL, maintenance, listener startup and completion budget         |
| retentionMs        | 604800000; positive safe integer or false; failures remain                    |
| onEvent            | Best-effort local observer, not awaited                                       |
| onHandlerError     | Awaited continue/stop decision, default continue                              |

Unless noted, milliseconds are integers 1–2147483647. retryDelayMs allows zero; retryMaxDelayMs and
shutdown grace also allow zero. Definition retries and execution deadlines are saved on submission,
while runner settings are local. [Reliability](reliability.md) covers fairness, ownership, capacity,
retention and external effects.

## Observation and error policy

| Event            | Evidence                                                                                                 |
| ---------------- | -------------------------------------------------------------------------------------------------------- |
| attemptStarted   | This runner invokes the handler after acquiring its scope                                                |
| attemptCancelled | Cancellation acknowledged after handler cleanup; recovery can also cancel lost claims                    |
| attemptCompleted | Completion acknowledged, including managed commit when applicable                                        |
| attemptFailed    | Failure bookkeeping acknowledged; original error and saved retryAt (Date/null)                           |
| attemptAbandoned | No confirmed outcome after cancellation; reason leaseLost/deadlineExceeded/runnerStopped/cancelRequested |
| runnerFailed     | Final startup/runtime/cleanup error, optional originating attempt                                        |

AttemptInfo contains id, name, attempt, maxAttempts and scheduledFor. Events omit payload/transaction;
identity metadata/Dates are detached. They are local/non-durable, exclude other runners and recovery,
and can be lost on process death. Unknown commits emit no false success. Confirmed completion may
precede a cleanup failure. Invalid input rejects without promising an event. Observer failures warn
as PGCLEAR_JOBS_OBSERVER_FAILED and cannot change outcomes; unresolved promises do not hold slots.

HandlerFailure adds original error, signal and transactional to AttemptInfo. onHandlerError runs after
confirmed managed rollback and before failure bookkeeping. Continue preserves normal JobError/retry
policy; stop rejects with the original value and leaves the claim for recovery. Managed opening/
commit/cleanup failures bypass the hook. A throwing/rejected policy or invalid action aggregates both
handler and policy errors. Decisions retain lease ownership while awaited; cancellation interrupts
waits/bookkeeping and observes late rejections. Callback side effects should honor signal.

JobError accepts retry:false or retryAt:Date, plus standard cause. retryAt is a minimum and does not
extend attempt budget; retry:false cannot combine with it. Async policies can explicitly return
Promise<HandlerErrorAction> to retain strict string-literal typing. Driver errors remain unchanged;
use pgclear's outcome evidence when choosing reconciliation/escalation policy.

## Storage exports

`installJobs(pool)` / `migrateJobs(pool,'up'?)` explicitly install/validate current storage;
`migrateJobs(pool,'down')` intentionally removes it. The CLI reads DATABASE_URL/environment or .env:
`pgclear-jobs migrate [up|down]`. Help needs no credentials. No worker/record facade performs DDL.

`jobsMigration.up/down` from `/migrations` is the single canonical installation/removal pair for the
current schema. The caller supplies the transaction. Applications author migrations for existing
storage themselves; no version-specific upgrade SQL is exported. [Storage](storage.md) covers
ownership, compatibility and grants.
