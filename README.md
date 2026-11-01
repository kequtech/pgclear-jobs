# Tenon Jobs

Typed, durable PostgreSQL jobs built on [pgclear](https://github.com/kequtech/pgclear),
with optional [Tenon ORM](https://github.com/kequtech/tenon-orm) integration.
Inspired by [pg-boss](https://github.com/timgit/pg-boss).

Requires Node.js 24+ and PostgreSQL 16+. Experimental; no npm release has been published.
Execution is **at least once**. Handlers must tolerate repeated effects.

## Define, submit, run

```ts
import { createPool } from '@kequtech/pgclear';
import { defineJob, installJobs, jobs, runJobs } from '@kequtech/tenon-jobs';

const connection = process.env.DATABASE_URL;
if (!connection) throw new Error('DATABASE_URL is required.');
const pool = createPool(connection);

// Explicit deployment step, before producers or runners start.
await installJobs(pool);

const reminder = defineJob({
  name: 'appointment-reminder',
  maxAttempts: 5,
  async handler(payload: { appointmentId: string }, { signal }) {
    await sendReminder(payload.appointmentId, signal);
  },
});

await jobs(pool).send(
  reminder,
  { appointmentId: 'appointment-42' },
  {
    key: 'appointment-42',
    delayMs: 60_000,
  },
);

const shutdown = new AbortController();
process.once('SIGTERM', () => shutdown.abort());
process.once('SIGINT', () => shutdown.abort());
try {
  await runJobs(pool, [reminder], {
    notifications: connection,
    signal: shutdown.signal,
    concurrency: 4,
  });
} finally {
  await pool.close();
}
```

`sendReminder` represents application code. Imports and definitions open no connections.
The runner is a long-running promise: observe its rejection and let app supervision restart it.
A supplied pool belongs to the application. Passing connection settings to `runJobs` instead
creates a runner-owned pool and notification listener.

## Join an application transaction

Core `jobs(pool)` and `jobs(tx)` use pgclear. For a Tenon database or transaction, use `/tenon`:

```ts
import { jobs } from '@kequtech/tenon-jobs/tenon';

await db.transaction(async (tx) => {
  const appointment = await saveAppointment(tx, input);
  await jobs(tx).send(
    reminder,
    { appointmentId: appointment.id },
    {
      key: appointment.id,
      runAt: appointment.reminderAt,
    },
  );
});
```

Commit exposes the job; rollback removes it with the app's writes. Handlers run independently
and never receive the submitting transaction. Tenon ORM is optional for the core package.

## Submit once, coalesce, or update

- `send(def, payload, { id })` uses a caller-selected UUID. Reusing it returns the original job,
  including after completion, for as long as that record is retained.
- `send(def, payload, { key })` returns unchanged pending or running work with that key.
  Terminal jobs release the key. IDs and keys cannot be combined.
- `upsert(def, payload, { key, delayMs })` inserts or replaces pending payload/timing atomically.
  Omitted timing preserves an existing execution time. Running work raises `JobStateError`.
- `sendMany(def, [{ id, payload }, ...])` inserts up to 1000 jobs in one statement, skips conflicts,
  and returns only inserted records.

All operations also work through `jobs(tx)`. Keep transactions short: a conflicting submission
may lock an outstanding record and delay its heartbeat.

## Retries and schedules

Thrown errors retry automatically with capped exponential backoff. Definitions support fixed
backoff, jitter, per-attempt timeouts and per-runner concurrency limits. For exceptions:

```ts
import { JobError } from '@kequtech/tenon-jobs';

throw new JobError('Invalid recipient', { retry: false });
// Or honor a provider's retry time without shortening policy backoff:
throw new JobError('Rate limited', { retryAt: new Date(retryAfter) });
```

Schedules are durable producers of ordinary jobs:

```ts
await jobs(pool).schedule(
  cleanupUploads,
  {},
  {
    key: 'hourly',
    cron: '0 * * * *',
    timeZone: 'UTC',
  },
);
```

Register at startup. An unchanged expression preserves its cursor across restarts. Defaults
create one catch-up occurrence after downtime and skip new occurrences while that schedule has
outstanding work. At least one runner selecting the definition must be running.

## Inspect and maintain

```ts
const counts = await jobs(pool).stats(reminder);
const page = await jobs(pool).find(reminder, { states: ['failed'], limit: 100 });
const last = page.at(-1);
const next = last
  ? await jobs(pool).find(reminder, {
      states: ['failed'],
      after: last.cursor,
      limit: 100,
    })
  : [];
const retry = await jobs(pool).retry(failedJobId);
// retry has a new ID and fresh budget; retry.retryOf links to the retained failure.
await jobs(pool).prune(reminder, { before: new Date(Date.now() - 7 * 86_400_000) });
```

Cleanup is explicit, bounded, and preserves failures unless requested. Removing a job also removes
its attempt history and releases its stable-ID deduplication. Store business results in app records.

## Development

```sh
npm ci
cp .env.example .env
npm run check
```

The example reuses Tenon ORM's local `tenon_orm_test` database. Tests require
`TENON_JOBS_TEST_DATABASE_URL` ending in `_test`; Jobs data lives in the reserved `tenon_jobs`
schema, and tests clean up their own definitions. CI checks Node.js 24 with PostgreSQL 16 and 18.

See the [API](docs/api.md), [reliability contract](docs/storage-and-reliability.md), and
[architecture](docs/architecture.md). Pending work lives in [todo.md](todo.md).
