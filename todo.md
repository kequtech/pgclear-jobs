# Pending work

## Package boundary and ORM integration

The package is pgclear-only with a required driver peer, a typed integration factory and one
up/down SQL pair. Follow the remaining [implementation plan](docs/implementation-plan.md) in Tenon
ORM: transaction cancellation options, the built-in Jobs facade with direct worker lifecycle forwarding, disabled-feature behavior, and
generation of application up/down SQL migrations when Jobs is explicitly enabled.

## Before a wider app migration

- [ ] Trial one repeatable maintenance job in an existing app alongside pg-boss. Exercise startup,
      scheduling, supervision, rollback, failure inspection, manual retry and shutdown with the app's
      real connection setup. Blockparty hourly cleanup is a suitable first candidate.
- [ ] Validate app payload compatibility across mixed app versions before adoption. Schema checks run
      only at runner startup. One up/down SQL pair is exported; validate a real application's
      rollout and code/schema compatibility, including any intentional down/up data loss.

## Implementation review

The simplified public API and reliability behavior are ready for app implementation review. Load
findings and reproduction commands are in the [reliability document](docs/storage-and-reliability.md#load-validation).
No further generic queue feature is required before that review. App review should confirm:

- Independent events use `send`, with a retained stable UUID where lost responses need reconciliation;
  latest-state work uses `request`. Business effects remain idempotent/reconcilable.
- Worker layout leaves capacity for short work alongside long editorial handlers. Choose existing
  concurrency limits or separate runners/pools, observe runner failures, and close pools after shutdown.
- Retention covers each workflow's reconciliation window, and runners sharing definitions agree on it.
- Queue cutover stops old producers/schedulers and drains or explicitly transfers outstanding old work,
  including delayed jobs and retries, before the replacement can execute the same workflow.

A private development integration is required before publication; a production canary remains a
separate step before wider adoption. Any deliberate schema reset must account for loss of queued
work and history; saved application migration files remain immutable.

## Cases to assess when an app needs them

- [ ] Site currently reads pg-boss handler output for generated-note status. Move that result/reference
      into app-owned records when migrating that workflow; avoid a second business-result store by default.
- [ ] Migrate Site editorial producers to keyed `request` only after app-owned forced-request
      arbitration is atomic. Preserve forced timing and intent when automatic requests arrive.
      The library replaces pending payloads; it does not merge business rules.
- [ ] Before the Site editorial canary, exercise an automatic job entering retry backoff, then an
      “analyse now” request, then another automatic request through the real app producers. Choose
      and assert when forced work must run relative to the original retries, and verify that later
      automatic requests cannot erase forced intent or postpone its chosen timing.
- [ ] Before the Site editorial canary, fill every handler slot with calls that exceed their execution
      deadlines. Pass the job abort signal through the actual editorial dependencies and verify that
      cancellation frees slots and queued work resumes. Also exercise a dependency that ignores
      cancellation: supervision must detect stalled queue progress and restart the worker even while
      HTTP remains healthy. Observe recovery and check effects remain idempotent after restart.
- [ ] Assess app-specific global pause, active cancellation and resuming cancelled work. The current
      API cancels pending jobs and supports process-level shutdown; it does not revoke external effects.
- [ ] Add schedule-list pagination if an app approaches the 1000-schedule listing limit.

## Scope

The main API is independent/stable-ID `send`, keyed latest-state `request`, application-owned pool
runners and recurring schedules with one catch-up/overlap behavior. Retries, recovery, cancellation
and completed/cancelled retention work by default. Optional settings change useful application
behavior; first-wins keyed sends, pending upsert and runner-owned query pools are intentionally absent.
Keep additions tied to app needs during the pilot. See the [API comparison](docs/architecture.md#api-scope-and-comparison-with-pg-boss).

## pg-boss comparison

Reviewed upstream documentation and source on 1 November 2026 (package metadata: 12.38.0).
The implemented common cases are durable/transactional and delayed submission, explicit IDs,
bulk insertion, keyed follow-ups, explicit pending edits, retries/backoff/jitter, leases, notifications, cron,
inspection, counts and bounded cleanup. These use the existing definition/record/runner model.

Other pg-boss cases do not justify copying its entire API:

| Case                                                                | Assessment                                                                                                                                                        |
| ------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Queue policies, strict per-key FIFO and distributed concurrency     | Keyed `request` serializes a head and its coalesced successor across runners. General FIFO and distributed concurrency limits remain separate needs.              |
| Priority, throttling and rate limits                                | Require a demonstrated fairness/provider-rate need. Backoff and Retry-After cover failure recovery, not proactive rate limiting.                                  |
| Dependency graphs and batch handlers                                | Ordinary follow-up submissions cover simple pipelines; durable graph failure/propagation semantics would be a separate feature. No current migration requires it. |
| Dead-letter routing                                                 | Failed records, diagnostics and explicit retry cover current operations. Another queue is not required for that workflow.                                         |
| Automatic pending expiry/retention                                  | Outstanding and failed work is retained. Completed/cancelled history expires after seven days by default, with an optional retention override.                    |
| Stored results, event/spies frameworks, dashboards, metrics history | Keep business results and observability integration app-owned; current direct statistics need no monitor service.                                                 |
| RRULE and multiple database engines                                 | Existing PostgreSQL and five-field cron foundations meet current requirements.                                                                                    |

Sources: [jobs](https://github.com/timgit/pg-boss/blob/master/docs/api/jobs.md),
[workers](https://github.com/timgit/pg-boss/blob/master/docs/api/workers.md),
[queues](https://github.com/timgit/pg-boss/blob/master/docs/api/queues.md), and
[SQL](https://github.com/timgit/pg-boss/blob/master/src/plans.ts), plus
[scheduling](https://github.com/timgit/pg-boss/blob/master/docs/api/scheduling.md) and
[backend options](https://github.com/timgit/pg-boss/blob/master/docs/api/constructor.md).
