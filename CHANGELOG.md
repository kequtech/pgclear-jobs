# Changelog

## 0.3.1

- Clarify repeated request behavior and worker troubleshooting with compact tables using existing
  queue state, lifecycle promises and diagnostic events.

## 0.3.0

- Process shared schedule request keys in a consistent lock order across parallel maintenance batches.
- Preserve earlier failure diagnostics consistently when cancellation is finalized by worker cleanup
  or lease recovery. Avoid waiting twice for cancelled handler cleanup.
- Extend cancel(id) to persist running-job cancellation across processes. Workers use the existing
  handler signal and heartbeat; completion, rollback, retry suppression and recovery remain fenced.
  Expose cancelRequestedAt and cancelled attempt history/events. Pending cancellation stays immediate.
- Require storage version 3. Export only the canonical jobsMigration.up/down for the current schema.
  Applications author changes to existing storage in new migrations after stopping old producers/workers;
  saved migration history remains unchanged. No version-specific upgrade SQL or upgrade command.
- Document cancellation races, detection latency and the distinction between queue state and external
  effects. Reuse one host shutdown signal; detailed driver contracts stay with pgclear.

## 0.2.2

- Simplify the README worker to a plain runJobs call. Keep optional process-signal wiring in hosting
  examples and clarify that one host signal can stop multiple runners while Jobs handles draining.
- Remove unnecessary SQL casts from worker examples. No runtime or storage changes.

## 0.2.1

- Rewrite standalone documentation around reference, storage, reliability, hosting and integration.
  Detailed queue contracts live here; maintainer plans/architecture stay outside the package.
- Move the generic Next process-host example and its real framework tests here from Tenon. Its
  worker uses only pgclear/Jobs, and the packed fixture explicitly has no Tenon dependency.

- Add bounded `getMany(ids)` lookups and optional `startJobs`/integration handles with `ready` and
  `done` promises, preserving runner and pool ownership.
- Add schedule `requestKey` so recurring, manual and startup triggers share existing keyed request
  semantics. Shared jobs have no exclusive schedule occurrence metadata.
- Storage version 2 adds the nullable schedule request key. Existing installations need an
  application-authored migration; stop old producers/workers first. Installation and Tenon's setup
  never upgrade or rewrite saved application migrations automatically.
- Ship a tested runtime-role grants example.

## 0.2.0

- Separate handler-free producer contracts from worker bindings with `JobSpec` and `defineWorker`;
  preserve inline definitions and infer native/integrated transaction scopes.
- Add isolated local `onEvent` observations and explicit `onHandlerError` continue/stop decisions
  after managed rollback. Preserve original errors, committed retry timestamps, cancellation, and
  concurrent cleanup failures; unknown commits never report successful completion.
- Add native runnable producer/worker examples and installed-package/type/reliability coverage.
- Keep schema SQL unchanged; this release needs no Jobs schema migration.

## 0.1.1

- Add optional, awaited `onReady` notification after storage validation and before worker dispatch.
  Startup callback failures preserve their original errors; shutdown interrupts pending callbacks
  and observes later rejections. Query-library integrations forward the option unchanged.

## 0.1.0

- Typed, inert definitions with saved execution/retry policy and validated JSON payloads.
- Independent/stable-ID submissions, bounded bulk insertion, and keyed requests with one coalesced
  successor behind running work and its retries.
- Application-owned pgclear scopes for transactional submissions and inspection/mutations.
- Opt-in transactional handlers commit writes, follow-up submissions, and successful completion
  together; errors, cancellation, and lost ownership roll back.
- Fenced claims, heartbeat leases, bounded recovery, execution deadlines, automatic retries, and
  graceful worker shutdown with propagation of driver/cleanup failures.
- Fair admission across definitions, per-definition concurrency caps, polling, optional notifications,
  durable cron schedules, and overlap arbitration including manual retries.
- Job/attempt inspection, precise cursor pagination, statistics, pending edits, explicit retry, and
  bounded cleanup. Automatic retention preserves failures and outstanding work.
- One install/remove schema command and exported up/down SQL for application migration files.
- Required pgclear peer and a typed integration factory with no ORM dependency.
- Separate attempt execution from worker dispatch and isolate shared input validation. Reject
  misspelled definition options before silently using default policy.
- Validate packaged runtime/types/CLI, rollback, competing workers, process death, lost ownership,
  uncertain commits, lost rollback responses, failure-heavy cleanup, and shared-pool behavior.
- Rewrite documentation around the current API, reliability contract, and thin query-library integration.
