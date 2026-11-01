# Changelog

## 0.1.0 — Unreleased

- Initial pgclear-backed job records and explicit schema installation.
- Typed definitions, policy snapshots and outstanding-work keys.
- Lookup, pending edits, rescheduling, cancellation and attempt inspection.
- Independent runners, retries, heartbeats, fenced claims and recovery.
- Optional Tenon transaction integration.
- Notification-assisted dispatch, delayed-job timers and reconnect reconciliation.
- Local slots remain occupied until timed-out or ownership-lost handlers settle.
- One bounded recovery loop per runner, independent of execution slots.
- Shared retry handling with configurable caps, optional jitter, `JobError` retry controls, and saved attempt-budget context.
- Durable cron schedules, time zones, previews, one catch-up occurrence and overlap suppression through the existing runner.
- Per-definition local concurrency and database-clock relative submission delays.
- Recheck claim expiry after row-lock waits; account for delayed claim/heartbeat responses and bound local leases.
- Reject PostgreSQL-incompatible payload text before SQL and sanitize persisted error messages.
- Stable submission UUIDs and single-statement bulk submission with conflict skipping.
- Precise cursor pagination, snapshot state counts, explicit terminal removal and bounded pruning.
- Manual retry creates linked fresh jobs while preserving failures, budgets and schedule context.
- Rewrite documentation around the implemented API; keep pending work and pg-boss case assessments in `todo.md`.
- Explicit keyed requests coalesce unstarted work or retain one durable follow-up behind a started job and all its retries.
- Inspectable and cancellable blocked successors, atomic release on terminal transitions, and transactional notification wakeups.
- Pending statistics distinguish ready, delayed and blocked work.
- Consolidate unreleased schema history into a fresh installation baseline; incompatible disposable schemas require explicit reset.
- Validate real TCP stalls and disconnects, uncertain pgclear/Tenon commits, stable-ID reconciliation, lease cancellation and lost completion responses without new runtime machinery.
- Simplify submission to independent/stable-ID send and keyed request; remove pending upsert and first-wins keyed send, including keyed bulk insertion.
- Require application-owned worker pools and make notification listeners consistently opt-in.
- Use one recurring schedule contract: one catch-up occurrence and suppression while outstanding work remains.
- Enable retry jitter by default and omit undefined object fields during JSON payload serialization.
- Automatically retain completed/cancelled history for seven days, with an extension/disable option; preserve failures and outstanding work.
- Refresh the disposable schema baseline to version 2 and reject removed options instead of silently changing behavior.
- Re-export the full API from the Tenon entry point so consumers need one library import path.
- Rotate claims between eligible definitions, preserving oldest-due-first selection within each definition without new scheduling options.
- Bound claim, next-due and retention scans by definition using existing indexes instead of sorting entire backlogs or scanning retained history.
- Add reproducible sustained-load profiles and fairness/shared-pool exhaustion regressions without changing the public API.
