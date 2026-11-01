# Pending work

## Before a wider app migration

- [ ] Trial one repeatable maintenance job in an existing app alongside pg-boss. Exercise startup,
      scheduling, supervision, rollback, failure inspection, manual retry and shutdown with the app's
      real connection setup. Blockparty hourly cleanup is a suitable first candidate.
- [ ] Validate app payload compatibility across mixed app versions before adoption. Schema checks run
      only at runner startup. This unreleased library has a fresh baseline with no upgrade migrations;
      design an upgrade path when installed data first needs to be retained.

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

A real-app canary is still required before wider adoption. Schema resets are only for disposable
installations; add a data-preserving schema/rollout path before the first upgrade that must retain work.

## Cases to assess when an app needs them

- [ ] Site currently reads pg-boss handler output for generated-note status. Move that result/reference
      into app-owned records when migrating that workflow; avoid a second business-result store by default.
- [ ] Migrate Site editorial producers to keyed `request` only after app-owned forced-request
      arbitration is atomic. Preserve forced timing and intent when automatic requests arrive.
      The library replaces pending payloads; it does not merge business rules.
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
