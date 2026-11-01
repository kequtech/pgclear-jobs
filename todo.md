# Pending work

## Before a wider app migration

- [ ] Trial one repeatable maintenance job in an existing app alongside pg-boss. Exercise startup,
      scheduling, supervision, rollback, failure inspection, manual retry and shutdown with the app's
      real connection setup. Blockparty hourly cleanup is a suitable first candidate.
- [ ] Define a guaranteed follow-up contract for keyed requests arriving while a handler runs.
      Site editorial debounce can update pending jobs now, but active work raises `JobStateError`.
      Preserve automatic retries without losing either the running payload or the latest requested work.
- [ ] Exercise network partitions and lost commit responses with a controllable connection proxy.
      Verify explicit-ID reconciliation and lease cancellation under real transport failures.
- [ ] Measure sustained submission/processing/cleanup load, backlog fairness and pool contention.
      Use the result to assess claim batch sizes and indexes; do not add partitioning speculatively.
- [ ] Validate mixed app versions during deployment. Schema checks reject old runners after upgrades;
      document a tested rollout sequence and check app payload compatibility.

## Cases to assess when an app needs them

- [ ] Site currently reads pg-boss handler output for generated-note status. Move that result/reference
      into app-owned records when migrating that workflow; avoid a second business-result store by default.
- [ ] Assess distributed per-key serialization before migrating jobs using pg-boss singleton policies.
      Local concurrency and outstanding submission keys do not replace cross-process execution limits.
- [ ] Assess app-specific global pause, active cancellation and resuming cancelled work. The current
      API cancels pending jobs and supports process-level shutdown; it does not revoke external effects.
- [ ] Add schedule-list pagination if an app approaches the 1000-schedule listing limit.

## pg-boss comparison

Reviewed upstream documentation and source on 1 November 2026 (package metadata: 12.38.0).
The implemented common cases are durable/transactional and delayed submission, explicit IDs,
bulk insertion, pending updates/upsert, retries/backoff/jitter, leases, notifications, cron,
inspection, counts and bounded cleanup. These use the existing definition/record/runner model.

Other pg-boss cases do not justify copying its entire API:

| Case                                                                | Assessment                                                                                                                                                        |
| ------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Queue policies, strict per-key FIFO and distributed concurrency     | Relevant only where app-level ordering/exclusivity requires them; see the migration assessment above.                                                             |
| Priority, throttling and rate limits                                | Require a demonstrated fairness/provider-rate need. Backoff and Retry-After cover failure recovery, not proactive rate limiting.                                  |
| Dependency graphs and batch handlers                                | Ordinary follow-up submissions cover simple pipelines; durable graph failure/propagation semantics would be a separate feature. No current migration requires it. |
| Dead-letter routing                                                 | Failed records, diagnostics and explicit retry cover current operations. Another queue is not required for that workflow.                                         |
| Automatic pending expiry/retention                                  | We deliberately retain outstanding work and make terminal cleanup explicit. A queue library should not silently discard app work.                                 |
| Stored results, event/spies frameworks, dashboards, metrics history | Keep business results and observability integration app-owned; current direct statistics need no monitor service.                                                 |
| RRULE and multiple database engines                                 | Existing PostgreSQL and five-field cron foundations meet current requirements.                                                                                    |

Sources: [jobs](https://github.com/timgit/pg-boss/blob/master/docs/api/jobs.md),
[workers](https://github.com/timgit/pg-boss/blob/master/docs/api/workers.md),
[queues](https://github.com/timgit/pg-boss/blob/master/docs/api/queues.md), and
[SQL](https://github.com/timgit/pg-boss/blob/master/src/plans.ts), plus
[scheduling](https://github.com/timgit/pg-boss/blob/master/docs/api/scheduling.md) and
[backend options](https://github.com/timgit/pg-boss/blob/master/docs/api/constructor.md).
