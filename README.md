# pgclear-jobs

Typed background jobs for PostgreSQL, built on [pgclear](https://github.com/kequtech/pgclear).
Install pgclear alongside this package; it is a required peer dependency.

Requires Node.js 24+ and PostgreSQL 16+. Experimental and unpublished.
Execution is **at least once**: handlers must tolerate repeated effects.

The package contains no ORM dependency. The [integration API](docs/integration.md) lets another
pgclear-backed query library supply its own transaction scopes to the same job engine. The
[implementation plan](docs/implementation-plan.md) tracks the built-in Jobs API planned for Tenon ORM.

## Define, send, run

Install storage once before starting the application. Set `DATABASE_URL` in the environment or `.env`:

```sh
npx pgclear-jobs migrate
```

An application can expose this as `"jobs:migrate": "pgclear-jobs migrate"` in its npm scripts.

```ts
import { createPool } from '@kequtech/pgclear';
import { defineJob, jobs, runJobs } from '@kequtech/pgclear-jobs';

const connection = process.env.DATABASE_URL;
if (!connection) throw new Error('DATABASE_URL is required.');
const pool = createPool(connection);

const sendReceipt = defineJob({
  name: 'send-receipt',
  async handler(payload: { orderId: string }, { signal }) {
    await emailReceipt(payload.orderId, signal);
  },
});

await jobs(pool).send(sendReceipt, { orderId: 'order-42' });

const shutdown = new AbortController();
process.once('SIGTERM', () => shutdown.abort());
process.once('SIGINT', () => shutdown.abort());
try {
  await runJobs(pool, [sendReceipt], { signal: shutdown.signal });
} finally {
  await pool.close();
}
```

`emailReceipt` represents application code. Definitions open no connections. The runner is a
long-running promise: observe its rejection and let application supervision restart it.
The application owns its pool; the runner never closes it.

By default, workers process one job at a time, retry failures up to three total attempts with
exponential backoff and jitter, enforce a five-minute execution limit, recover abandoned work,
and clean up completed/cancelled history after seven days. Failures remain available for inspection.

## Commit jobs with application writes

Use the transaction supplied by your pgclear pool:

```ts
await pool.transaction(async (tx) => {
  const order = await saveOrder(tx, input);
  await jobs(tx).send(sendReceipt, { orderId: order.id });
});
```

The job commits or rolls back with the order. Handlers execute independently of the submitting
transaction. `saveOrder` represents application code using that same transaction.

## Commit handler writes with completion

Opt into `transactional: true` for short database handlers. The runner supplies `tx`, then commits
its writes and successful job completion together:

```ts
import { sql } from '@kequtech/pgclear';

const markProjectReady = defineJob({
  name: 'mark-project-ready',
  transactional: true,
  async handler(payload: { projectId: string }, { tx }) {
    await tx.query(sql`UPDATE projects SET ready = true WHERE id = ${payload.projectId}`);
  },
});
```

The supplied `tx` is a pgclear `Transaction` and works with `jobs(tx)` for follow-up submissions. Handler errors and lost
ownership roll back the transaction; failure/retry history is recorded separately. Calls through a
separate `db` or pool and external effects remain outside it. Transactional handlers hold a connection
until commit or rollback, so reserve pool capacity for claims, renewals and application traffic.
Use ordinary handlers for long external calls. See the [transaction contract](docs/api.md#transactional-handlers).

## Send events or request the latest state

Use `send` when each event needs processing. Keep a stable UUID when the same submission may be
retried after a lost response:

```ts
await jobs(pool).send(sendReceipt, { orderId }, { id: submissionId });
```

Reusing that UUID returns the original job, including after completion, while its record is retained.
Choose the UUID once and retain it outside an uncertain transaction. Business writes also need
application-owned reconciliation; see the [reliability contract](docs/storage-and-reliability.md).

Use `request` for work such as rebuilding an entity from its latest source data:

```ts
await jobs(pool).request(rebuildProject, { projectId }, { key: projectId, delayMs: 60_000 });
```

Pending work that has never started is updated with the latest payload and supplied timing. Once
execution starts, requests keep one follow-up behind that job and all its retries. The current
payload is preserved. The follow-up becomes eligible after the current job terminates, with a fresh
attempt budget. Multiple requests coalesce into that follow-up; they do not each create a run.

Both methods accept `runAt: Date` or `delayMs`. Payloads are JSON data; absent object fields with
`undefined` values are omitted. Dates and other lossy values are rejected. Application rules such
as preserving a forced request belong in application transactions.

## Schedule recurring work

```ts
await jobs(pool).schedule(cleanupUploads, {}, { cron: '0 * * * *' });
```

Register schedules at startup. Repeating an unchanged schedule preserves its cursor. After downtime,
one catch-up occurrence is submitted. New occurrences are skipped while that schedule still has
outstanding work. Cron defaults to UTC; supply `timeZone` for a local calendar. A runner selecting
the definition must be running.

## Adjust useful behavior

Put execution policy on the definition, where every submission snapshots it:

```ts
const analyzeProject = defineJob({
  name: 'analyze-project',
  maxAttempts: 4,
  executionTimeoutMs: 2 * 60 * 60 * 1000,
  async handler(payload: { projectId: string }, { signal }) {
    await analyze(payload.projectId, signal);
  },
});
```

For a larger worker, set `concurrency`. For faster dispatch, optionally supply a dedicated listener
connection. Polling remains the fallback. Extend or disable automatic retention when needed:

```ts
await runJobs(pool, [sendReceipt, analyzeProject], {
  signal: shutdown.signal,
  concurrency: 4,
  notifications: connection,
  retentionMs: 30 * 86_400_000, // Or false to retain completed/cancelled history.
});
```

A definition can also cap its concurrency within each runner. These limits are local to a runner;
keyed requests serialize valid database claims across runners. Each runner rotates between eligible
definitions so an older backlog gets no monopoly on new slots. Running handlers are not preempted;
reserve room for short work with per-definition concurrency limits or separate runners when needed.
Size the shared pool for application/handler SQL as well as queue operations. External effects still
need idempotency.

Use `JobError` for permanent errors or a provider's retry time. Inspection, explicit retry, pending
edits, bulk submission and manual cleanup are available through the same `jobs(scope)` facade;
see the [API reference](docs/api.md).

## Development

```sh
npm ci
cp .env.example .env
npm run check
```

Tests require `PGCLEAR_JOBS_TEST_DATABASE_URL` ending in `_test`. The example reuses an existing local
test database. Migration tests intentionally exercise removal and reinstall of the Jobs schema;
use a disposable test installation. CI checks PostgreSQL 16 and 18.

Run `npm run test:load` for optional sustained-load measurements against the test database.
The normal test suite includes deterministic fairness and pool-contention regressions.

`pgclear-jobs migrate` installs the schema; `pgclear-jobs migrate down` intentionally removes it.
The package exports one concrete up/down SQL pair for application migrations.
See [schema migrations](docs/migrations.md).

See [architecture](docs/architecture.md), [reliability](docs/storage-and-reliability.md), and
[pending adoption checks](todo.md).
