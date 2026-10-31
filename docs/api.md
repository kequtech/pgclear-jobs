# Public API and application workflow

The intended public model has three pieces:

1. `defineJob(...)` describes behavior once.
2. `jobs(dbOrTx)` reads and changes persisted records through a supplied database scope.
3. `runJobs(pool, definitions)` processes committed records independently.

The examples are design examples, not executable instructions for a published package.
Application functions such as `saveAppointment` stand for ordinary application code.

## Define behavior once

```ts
// appointment-reminder.ts
import { defineJob } from '@kequtech/tenon-jobs';
import { sendAppointmentReminder } from './application-mail.js';

export const appointmentReminder = defineJob({
  name: 'appointment-reminder',
  maxAttempts: 5,
  retryDelayMs: 30_000,
  retryBackoff: 'exponential',
  executionTimeoutMs: 60_000,

  async handler({ appointmentId }: { appointmentId: string }) {
    await sendAppointmentReminder(appointmentId);
  },
});
```

The stable name, typed handler, and execution policy belong together. Submissions supply data and
instance-specific timing; they do not redeclare behavior. The illustrated policy option names and
backoff contract still need specification. The working meaning of `maxAttempts: 5` is five total
attempts, including the first.

Defining or importing a job does not open connections or start processing. A definition does not
require a database connection. The handler remains application code; it is never serialized into
PostgreSQL. Payloads are persisted data, provisionally JSON. Serialization and runtime validation
rules remain to be specified.

## Submit through an explicit database scope

```ts
import { jobs } from '@kequtech/tenon-jobs/tenon';
import { appointmentReminder } from './appointment-reminder.js';

const reminderJob = await db.transaction(async (tx) => {
  const appointment = await saveAppointment(tx, input);

  const job = await jobs(tx).send(
    appointmentReminder,
    { appointmentId: appointment.id },
    {
      key: appointment.id,
      runAt: appointment.reminderAt,
    },
  );

  await saveReminderJobId(tx, appointment.id, job.id);
  return job;
});
```

The application writes and queue insertion commit or roll back together. A returned record inside
the callback does not prove the outer transaction committed. `send` inserts a record; it never
invokes the handler.

`send` returns the job directly. There is no `{ job, created }` wrapper. A job's state does not
identify whether this call created it: a new job and an existing job may both be pending.

Without an application transaction, the scope is still explicit:

```ts
const job = await jobs(db).send(appointmentReminder, { appointmentId });
```

For direct pgclear use, import the core entry point:

```ts
import { jobs } from '@kequtech/tenon-jobs';

await jobs(pool).send(appointmentReminder, { appointmentId });
await pool.transaction(async (transaction) => {
  await jobs(transaction).send(appointmentReminder, { appointmentId });
});
```

`jobs(scope)` creates a lightweight record-operation facade. It does not open a new pool, begin a
transaction, or start a runner. All of its operations use the supplied scope.

## Find existing work

Lookup is a first-class requirement. Applications may need to inspect work already in the pipeline,
reuse a job ID, or decide how to respond to pending or running work.

```ts
const job = await jobs(db).get(reminderJobId);

const outstanding = await jobs(db).find(appointmentReminder, {
  key: appointmentId,
  states: ['pending', 'running'],
  limit: 20,
});

const failures = await jobs(db).find(appointmentReminder, {
  states: ['failed'],
  limit: 50,
});
```

The proposed contract is that `get` returns a record or `null`, and `find` returns a bounded list.
Exact filters, ordering, pagination, record types, and error types are not finalized. A supplied
definition should preserve the payload type in definition-specific operations.

Storing the returned job ID on the owning application record is a straightforward lookup strategy.
An application key is another. Arbitrary JSON payload searching is not required for basic lookup.

Lookup alone does not prevent duplicates: two callers can both find no matching job. See the
[proposed key contract](storage-and-reliability.md#outstanding-work-keys) for atomic prevention.

## Change existing work

```ts
await db.transaction(async (tx) => {
  await changeAppointmentTime(tx, appointmentId, newTime);
  await jobs(tx).reschedule(reminderJobId, newReminderTime);
});

await db.transaction(async (tx) => {
  await cancelAppointment(tx, appointmentId);
  await jobs(tx).cancel(reminderJobId);
});
```

The working proposal permits payload edits, rescheduling, and cancellation of pending jobs. The
mutation itself must enforce eligibility; a preceding read cannot establish that a job is still
pending. Typed not-found and state-conflict errors were proposed but not finalized. An escaping
error rolls back the application's transaction under Tenon's normal rules.

Active-job cancellation is unresolved. Cancellation or deletion cannot undo handler effects.
The exact APIs for payload updates, removal, and manual retry still need design.

## Run jobs explicitly

```ts
// run-jobs.ts
import { createPool } from '@kequtech/pgclear';
import { runJobs } from '@kequtech/tenon-jobs';
import { appointmentReminder } from './appointment-reminder.js';

const pool = createPool({
  connectionString: process.env.DATABASE_URL,
});

await runJobs(pool, [appointmentReminder]);
```

`runJobs` is a long-running function. It claims eligible records for the supplied definitions,
executes their handlers, and records outcomes. The definitions already contain the handlers;
the list selects work this runner can execute rather than separately registering behavior.

The runner may execute alongside an application in the same process and event loop, or in a
separate process. Different processes may select different definitions, or select the same ones
and share work. All use the same database tables.

Starting a runner in the application must not prevent the server from starting: retain and observe
its running promise rather than awaiting it before server startup. A runner does not automatically
move CPU-heavy handlers off the event loop; use a separate process when that isolation is needed.

The runner uses its own process's pool and never receives a producer's transaction. Returning from
a handler completes an attempt; throwing follows its retry policy. Exhausted work stays failed and
inspectable. An `attempts(jobId)` inspection method was proposed; its exact shape remains open.

An `AbortSignal` is the leading shutdown interface, but its signature and contract are unresolved.
The runner must stop new claims and handle unfinished handlers deliberately. A supplied pool is
borrowed: the application closes it after runner shutdown. No public worker class is required by
the accepted API direction.
