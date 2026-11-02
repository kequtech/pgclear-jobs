# pgclear-jobs

Durable PostgreSQL jobs with typed handlers, transactional submissions, retries, recurring schedules,
and keyed follow-up work. Built on [pgclear](https://github.com/kequtech/pgclear).

ESM; Node.js 24+; PostgreSQL 16+.

Execution is **at least once**: handlers must tolerate repeated execution, including after a worker
loses its lease.

## Install and run

Install both packages; pgclear is a required peer dependency:

```sh
npm install @kequtech/pgclear-jobs @kequtech/pgclear
```

Set `DATABASE_URL` in the environment or `.env`, then install the Jobs schema:

```sh
npx pgclear-jobs migrate
```

A worker is an ordinary async function using your existing pool:

```ts
import { createPool } from '@kequtech/pgclear';
import { defineJob, jobs, runJobs } from '@kequtech/pgclear-jobs';

const connection = process.env.DATABASE_URL;
if (!connection) throw new Error('DATABASE_URL is required.');
const pool = createPool(connection);

const greet = defineJob({
  name: 'greet',
  async handler(payload: { name: string }, { signal }) {
    signal.throwIfAborted();
    console.log(`Hello, ${payload.name}`);
  },
});

await jobs(pool).send(greet, { name: 'Ada' });

const stop = new AbortController();
process.once('SIGTERM', () => stop.abort());
process.once('SIGINT', () => stop.abort());

try {
  await runJobs(pool, [greet], { signal: stop.signal });
} finally {
  await pool.close();
}
```

The application loads its own environment before creating the pool. The migration command reads
`.env` itself. Imports and definitions open no connections and perform no schema changes.

A runner owns claiming, heartbeats, retries, schedule advancement, retention, notifications, and
worker shutdown. It processes only the supplied definitions, with one unfinished handler at a time
by default. Observe its promise: runner infrastructure and managed-transaction failures reject it
for application supervision.
The runner leaves the application pool open.

HTTP hosts can use `startJobs(pool, definitions, options)` for `{ ready, done }` promises, or retain
`runJobs` with its `onReady` callback. Both use the same runner and leave pool ownership unchanged.
See [startup readiness](docs/api.md#startup-readiness).

Existing Jobs 0.2.0 installations need the explicit version-1-to-2 upgrade SQL before running
Jobs 0.2.1. New installation SQL is separate; never reset queued work to upgrade. See
[storage upgrades and grants](docs/migrations.md). Bulk status reads can use `jobs(pool).getMany(ids)`
for up to 1000 IDs in one statement, in requested order with duplicates/missing records omitted.

## Share a producer contract and bind workers separately

A producer only needs the name, payload type, and submission policy:

```ts
// jobs.ts — shared with producers; imports no handler implementation.
export const refresh = defineJob<{ projectId: string }>({
  name: 'refresh-project',
  maxAttempts: 4,
});

await jobs(pool).send(refresh, { projectId });
```

Bind the handler in the worker process:

```ts
import { defineWorker } from '@kequtech/pgclear-jobs';

const worker = defineWorker(refresh, {
  transactional: true,
  async handler(payload, { tx }) {
    await tx.queryClassic('UPDATE projects SET ready = true WHERE id = $1', [payload.projectId]);
  },
});
await runJobs(pool, [worker], { signal });
```

`defineWorker` preserves the shared submission policy and payload type. It adds only handler,
transactional mode, and local concurrency. Inline `defineJob({ handler })` remains supported.
A handler-free contract cannot be registered with a runner. See the
[runnable producer/worker example](examples/README.md).

## Submit events or request current state

`send` creates an independent event. Reuse a stable UUID when reconciling an uncertain submission:

```ts
await jobs(pool).send(greet, { name: 'Ada' }, { id: submissionId, delayMs: 1000 });
```

Keep `submissionId` outside the transaction being reconciled. Reusing it returns the original record
while that record is retained, even after completion.

`request` coalesces work by definition and key:

```ts
await jobs(pool).request(greet, { name: 'Ada' }, { key: 'account-42' });
```

An unstarted pending job gets the latest payload. After any attempt starts, further requests retain
one successor behind the current job and all its retries. Requests update that successor; terminal
completion releases it atomically with a fresh attempt budget. This suits rebuilding something
from current application state. Each request does not promise a separate execution.

Both methods accept either `runAt: Date` or `delayMs`. Payloads must be JSON data. Undefined object
fields are omitted; Dates, undefined array elements, cycles, and other lossy values are rejected.

## Use the same transaction

Submit through `jobs(tx)` to commit a job with application writes:

```ts
await pool.transaction(async (tx) => {
  await saveAccount(tx, account);
  await jobs(tx).send(greet, { name: account.name });
});
```

`saveAccount` is application code using that transaction. Rollback removes both writes and the job.

Short database handlers can also commit their writes and successful completion together:

```ts
const markReady = defineJob({
  name: 'mark-ready',
  transactional: true,
  async handler(payload: { projectId: string }, { tx }) {
    await tx.queryClassic('UPDATE projects SET ready = true WHERE id = $1', [payload.projectId]);
  },
});
```

The application owns the `projects` table. Use the supplied `tx` for every participating write and
follow-up submission. Errors and lost ownership roll those writes back. Other pools, separate
transactions, and external effects are outside this guarantee. A transactional handler holds a
connection until commit or rollback; leave capacity for heartbeats and application queries.

## Schedules and execution policy

```ts
await jobs(pool).schedule(
  greet,
  { name: 'Ada' },
  { cron: '0 9 * * *', timeZone: 'Europe/Budapest' },
);
```

Registering an unchanged schedule preserves its cursor. Schedules submit one catch-up occurrence
after downtime and suppress new occurrences while previous work remains outstanding. A runner
selecting the definition must be running to advance its schedules. Set `requestKey` separately from
the schedule identity `key` when scheduled/manual/startup triggers should share a keyed request.
Ticks preserve existing timing and retries; each schedule records the affected shared job's ID.
See [schedule semantics](docs/api.md#recurring-schedules).

Definitions default to three total attempts, exponential backoff with jitter, and a five-minute
execution deadline. Set policy on `defineJob`; each submission snapshots it. Use `JobError` to mark
a failure permanent or set a minimum retry time.

Runner options configure concurrency, an optional notification connection, shutdown grace, and
retention. Completed/cancelled history is retained for seven days by default; failures remain until
explicit cleanup. Cleanup also releases stable-ID deduplication. Pass cancellation signals through
your dependencies: JavaScript that ignores cancellation can continue after its claim is lost.

Use `onEvent` for local observations and `onHandlerError` for explicit continue/stop decisions.
See [observation and error policy](docs/api.md#observation-and-error-policy).

## Documentation

- [API reference](docs/api.md): definitions, records, schedules, errors, and runner options.
- [Reliability](docs/storage-and-reliability.md): atomicity, leases, uncertain commits, and shutdown.
- [Schema setup](docs/migrations.md): the migrate command and one exported up/down SQL pair.
- [Integration](docs/integration.md): bind another pgclear-backed query API to this engine.
- [Architecture](docs/architecture.md) and [development](docs/development.md).

The package has no ORM dependency. Tenon ORM exposes its thin `db.jobs`/`tx.jobs` facade over this
engine. See the [integration contract](docs/integration.md).
