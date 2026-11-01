# Remaining work

The pgclear-only API, worker lifecycle, transactional handlers, one schema pair, and integration
factory are implemented. Keep changes tied to demonstrated reliability or application needs.

## Tenon ORM

Tenon ORM implements its guarded query/transaction binding, `db.jobs`/`tx.jobs` facade,
disabled-feature behavior, and application migration generation. Keep those integration checks in
Tenon; Jobs retains native pgclear and custom-scope engine coverage.

Jobs 0.1.1 adds the shared `onReady` startup callback for HTTP hosts. Validate it through Tenon's
forwarded runner options using the selected package artifact. Continue supervising the lifetime
promise after startup. See the [integration responsibilities](docs/implementation-plan.md).

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

Follow the application acceptance checks above as consuming workflows adopt Jobs. See [development and release](docs/development.md).

## Optional improvements when needed

Continuous worker progress reporting and structured `JobStateError` reasons may help administration
and stall diagnosis. Startup notification is available through `onReady`; ongoing health reporting
remains a separate capability to shape around concrete needs. Larger queue
features, alternate database backends, stored business results, and a plugin framework are outside
current needs.
