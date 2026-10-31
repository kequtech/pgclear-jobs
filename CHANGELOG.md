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
