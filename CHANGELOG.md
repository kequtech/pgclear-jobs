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
- Transactional schema version 2 upgrade preserving existing jobs and attempts.
- Shared retry handling with configurable caps, optional jitter, `JobError` retry controls, and saved attempt-budget context.
- Durable cron schedules, time zones, previews, bounded catch-up and overlap policies through the existing runner.
- Per-definition local concurrency and database-clock relative submission delays.
- Schema version 3 adds schedules and job occurrence metadata without rewriting existing policies.
- Recheck claim expiry after row-lock waits; account for delayed claim/heartbeat responses and bound local leases.
- Reject PostgreSQL-incompatible payload text before SQL and sanitize persisted error messages.
- Stable submission UUIDs and single-statement bulk submission with conflict skipping.
- Atomic pending upsert with explicit active-work conflicts and preserved execution policy.
- Precise cursor pagination, snapshot state counts, explicit terminal removal and bounded pruning.
- Manual retry creates linked fresh jobs while preserving failures, budgets and schedule context.
- Schema version 4 adds retry links, occurrence-index rules and a terminal-cleanup index.
- Rewrite documentation around the implemented API; keep pending work and pg-boss case assessments in `todo.md`.
