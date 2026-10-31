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
- Attempt history, retained terminal records and graceful shutdown with a deadline.
- Explicit, transactional schema installation and upgrades with a version guard.

Recurring schedules, manual retry, removal, retention controls, a worker registry and aggregate
operational statistics are not implemented.
Execution is **at least once**: handlers must account for repeated external effects.

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
