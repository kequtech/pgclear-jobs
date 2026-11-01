# Remaining work

The pgclear-only API, worker lifecycle, transactional handlers, one schema pair, and integration
factory are implemented. Keep changes tied to demonstrated reliability or application needs.

## Tenon ORM

Follow the [implementation plan](docs/implementation-plan.md): transaction-options forwarding,
one query/transaction binding, the thin `db.jobs`/`tx.jobs` facade, disabled behavior, and migration
file generation. Validate actual ORM guards and lifecycle through packed or published packages.
The current Jobs tests use native pgclear and custom scopes, not the future Tenon implementation.

## Application acceptance

- Exercise a real maintenance workflow with startup, supervision, inspection, retry, retention,
  cancellation, and shutdown using the application's real dependencies and connection setup.
- Preserve business idempotency and stable submission IDs across uncertain outcomes. Keep history
  longer than the reconciliation window; persist generated results/references in application records.
- For forced keyed work, test an automatic attempt entering backoff, a forced request, then another
  automatic request. Choose the timing rule and preserve forced intent atomically in app-owned state.
- Fill handler slots with long calls. Verify actual dependency cancellation restores progress, and
  that supervision detects uncooperative stalls even while an HTTP server remains healthy.
- Reserve capacity for short work and size the pool for application SQL plus queue operations.
- During queue cutover, stop old producers/schedulers and account for delayed, retrying, and active
  work before both systems can execute the same workflow. Verify payload compatibility across app versions.
- Any intentional schema reset must account for lost jobs/history; saved application migration files
  remain immutable. Validate the deployment's transport/failover behavior where applicable.

Publication can precede Tenon integration. Follow the application acceptance checks above as the
integration is implemented. See [development and release](docs/development.md).

## Optional improvements when needed

Worker readiness/progress reporting and structured `JobStateError` reasons may help administration
and stall diagnosis. Add them in Jobs when a consuming workflow establishes the shape. Larger queue
features, alternate database backends, stored business results, and a plugin framework are outside
current needs.
