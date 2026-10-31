# Tenon Jobs

An experimental TypeScript library for durable PostgreSQL jobs. A sister to
[Tenon ORM](https://github.com/kequtech/tenon-orm), built directly on
[pgclear](https://github.com/kequtech/pgclear), inspired by
[pg-boss](https://github.com/timgit/pg-boss).

Define behavior once, submit through an explicit database scope, and run committed jobs
independently. Requires Node.js 24+ and PostgreSQL 16+. This is an initial implementation,
not a production-ready replacement for pg-boss. No npm release has been published by this project.

```ts
import { createPool } from '@kequtech/pgclear';
import { defineJob, installJobs, jobs, runJobs } from '@kequtech/tenon-jobs';

const connection = process.env.DATABASE_URL;
if (!connection) throw new Error('DATABASE_URL is required.');
const pool = createPool(connection);

// Explicit deployment step; run before producers or consumers start.
await installJobs(pool);

const reminder = defineJob({
  name: 'appointment-reminder',
  maxAttempts: 5,
  async handler(payload: { appointmentId: string }, { signal }) {
    // Call application code here; use signal for cooperative cancellation.
    await sendReminder(payload.appointmentId, signal);
  },
});

const job = await jobs(pool).send(
  reminder,
  { appointmentId: 'appointment-42' },
  {
    key: 'appointment-42',
    runAt: new Date(Date.now() + 60_000),
  },
);

await jobs(pool).reschedule(job.id, new Date(Date.now() + 120_000));
console.log(await jobs(pool).get(job.id));

// Run alongside your server, or in a separate process importing the definitions.
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

`sendReminder` represents application code. Importing the module or defining a job does not
open connections or start workers. The application owns the supplied pool. The runner owns its
dedicated notification connection and closes it on shutdown. A standalone consumer can instead
call `runJobs(connection, [reminder], options)` to let the runner own both its pool and listener.

## Transactions and Tenon

Core `jobs(pool)` and `jobs(transaction)` use the supplied pgclear scope. For Tenon, import
the optional integration; it uses Tenon's public query API and joins its transaction:

```ts
import { jobs } from '@kequtech/tenon-jobs/tenon';

await db.transaction(async (tx) => {
  const appointment = await saveAppointment(tx, input);
  const job = await jobs(tx).send(
    reminder,
    { appointmentId: appointment.id },
    {
      key: appointment.id,
      runAt: appointment.reminderAt,
    },
  );
  await saveReminderJobId(tx, appointment.id, job.id);
});
```

Commit makes the job visible; rollback removes it along with the application's writes.
The handler runs later in an independent worker. It receives the persisted payload and
an attempt context, never the submitting transaction. Tenon ORM is an optional peer and is
not required by the core entry point or its declarations.

## Current capabilities

- JSON payloads and typed definitions; durable and delayed submission.
- Outstanding-work keys, scoped to the definition name.
- Bounded lookup, pending-only edits, rescheduling and cancellation.
- Independent runners with bounded local handler concurrency and definition selection.
- Transactional notifications, reconnect reconciliation, delayed-job timers and fallback polling.
- Bounded retries, policy snapshots, claim fencing, heartbeats and expired-claim recovery.
- Capped exponential retry backoff, optional jitter, and provider-directed retry timing.
- Recurring cron schedules with time zones, bounded catch-up, and overlap control.
- Attempt history, retained terminal records and graceful shutdown with a deadline.
- Explicit, transactional schema installation and upgrades with a version guard.

Manual retry, removal, retention controls, a worker registry and aggregate operational statistics
are not implemented.
Execution is **at least once**: handlers must account for repeated external effects.

## Retry policy

Thrown errors retry automatically. Define the policy once; the runner persists the next attempt's
time and frees the slot while waiting. Handlers do not need timers, sleeps or queue-record edits.

```ts
const syncAccount = defineJob({
  name: 'sync-account',
  maxAttempts: 8,
  retryDelayMs: 1000,
  retryMaxDelayMs: 60_000,
  retryJitter: true,
  concurrency: 2,
  async handler(payload: { accountId: string }, context) {
    await synchronizeAccount(payload.accountId, context.signal);
  },
});
```

Exponential backoff is the default. Without jitter, delays grow as 1, 2, 4, 8 seconds and so on,
up to the configured cap. Optional jitter spreads each delay between half and all of that value.
`context.attempt` and the saved `context.maxAttempts` describe the current attempt budget.
The definition's concurrency is a per-runner limit within the runner's overall concurrency.

For the exceptions to ordinary automatic retries:

```ts
import { JobError } from '@kequtech/tenon-jobs';

throw new JobError('Invalid recipient', { retry: false });
// Or honor a provider's Retry-After time without shortening the normal backoff:
throw new JobError('Provider rate limited', { retryAt: new Date(retryAfter) });
```

Neither option extends the attempt budget. `executionTimeoutMs` still bounds an individual attempt;
retry delays control when a subsequent attempt starts. Use `{ delayMs: 60_000 }` on `send` for an
initial relative delay, or `runAt` for an absolute time.

## Recurring jobs

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

const schedules = await jobs(pool).schedules(cleanupUploads);
await jobs(pool).unschedule(cleanupUploads, 'hourly');
```

`cleanupUploads` is an ordinary job definition. Schedules create ordinary jobs with the same
handlers, retry policy and inspection API. Registration is safe on every startup: the same name
and schedule key update the existing schedule while preserving an unchanged expression's cursor.
Schedule writes also work through `jobs(tx)` in core and `/tenon`.

Schedules use deterministic five-field cron expressions, with UTC as the default time zone.
`previewSchedule({ cron, timeZone }, { from, count })` returns upcoming Dates without a database.
The default missed-run policy creates one catch-up job for the saved due occurrence, then resumes
at the next future occurrence. It never replays a downtime backlog. Set `missed: 'skip'` to discard
an occurrence more than one minute late. By default, an outstanding job from the same schedule
causes the new occurrence to be skipped; `overlap: 'allow'` permits another job.

At least one runner selecting the definition must be running. Its existing maintenance loop
advances due schedules transactionally; there is no separate scheduling service. Generated jobs
expose `scheduleId` and `scheduledFor`, and handlers receive `context.scheduledFor`. Removing a
schedule stops future submissions and leaves already submitted jobs intact.

## Development

```sh
npm ci
cp .env.example .env
# The example reuses Tenon ORM's local tenon_orm_test database.
npm run check
```

Tests require `TENON_JOBS_TEST_DATABASE_URL` and refuse database names without a `_test` suffix.
The local example shares Tenon ORM's test database, with Jobs tables isolated in the `tenon_jobs`
schema. You can configure another test database instead. Tests install app-owned tables and clean
up only their own job definitions. They exercise real PostgreSQL transactions, competing workers,
process death, deadlines, notification delivery, reconnects, schema upgrades and shutdown. No
development database is needed for this library. CI checks PostgreSQL 16 and 18 on Node.js 24.

See the [implemented API and reliability contract](docs/implementation.md). The other documents
preserve the initial design discussion, including proposals not yet implemented.
