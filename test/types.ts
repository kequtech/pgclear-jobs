import { createPool } from '@kequtech/pgclear';
import { defineJob, defineWorker, jobs, runJobs } from '../src/index.js';

// Compile-time consumer contract; never executed.
async function types() {
  const pool = createPool({});
  const a = defineJob({
    name: 'a',
    handler(payload: { id: string }) {
      void payload.id;
    },
  });
  const b = defineJob({
    name: 'b',
    handler(payload: number) {
      void payload;
    },
  });
  const transactional = defineJob({
    name: 'transactional',
    transactional: true,
    async handler(payload: { id: string }, { tx }) {
      await tx.queryClassic('SELECT $1::text', [payload.id]);
      await tx.savepoint(async (nested) => {
        await jobs(nested).send(a, payload);
      });
    },
  });
  defineJob({
    name: 'ordinary',
    handler(_payload: number, context) {
      // @ts-expect-error Ordinary handlers do not receive a transaction.
      void context.tx;
    },
  });
  await jobs(pool).send(transactional, { id: 'one' });
  // @ts-expect-error Transactional payload types remain bound to their definition.
  await jobs(pool).send(transactional, { id: 1 });
  await runJobs(pool, [a, transactional]);
  const job = await jobs(pool).send(a, { id: '1' });
  const id: string = job.payload.id;
  void id;
  // @ts-expect-error A payload cannot widen the definition's input type.
  await jobs(pool).send(a, { id: 3 });
  // @ts-expect-error Typed updates use the definition's payload.
  await jobs(pool).update(a, job.id, { id: false });
  await jobs(pool).send(a, { id: '1' }, { id: '00000000-0000-0000-0000-000000000001' });
  // @ts-expect-error Stable IDs and outstanding keys have different lifetimes.
  await jobs(pool).send(a, { id: '1' }, { id: 'id', key: 'key' });
  const batch = await jobs(pool).sendMany(a, [{ payload: { id: '1' } }]);
  const batchId: string | undefined = batch[0]?.payload.id;
  void batchId;
  // @ts-expect-error A batch cannot widen the definition's payload.
  await jobs(pool).sendMany(a, [{ payload: { id: 1 } }]);
  // @ts-expect-error Keyed work uses request, not another update-or-insert contract.
  await jobs(pool).upsert(a, { id: '1' }, { key: 'one' });
  // @ts-expect-error Bulk events do not coalesce by key.
  await jobs(pool).sendMany(a, [{ payload: { id: '1' }, key: 'one' }]);
  const requested = await jobs(pool).request(a, { id: 'latest' }, { key: 'one' });
  const requestedId: string = requested.payload.id;
  const blocker: string | null = requested.blockedBy;
  void requestedId;
  void blocker;
  // @ts-expect-error Requests require a key.
  await jobs(pool).request(a, { id: 'latest' }, {});
  // @ts-expect-error Requests cannot widen the definition's payload.
  await jobs(pool).request(a, { id: 1 }, { key: 'one' });
  // @ts-expect-error Request identity is assigned by the queue, not a stable submission ID.
  await jobs(pool).request(a, { id: 'latest' }, { key: 'one', id: 'id' });
  await jobs(pool).find(a, { after: job.cursor });
  // @ts-expect-error Cleanup only accepts terminal states.
  await jobs(pool).prune(a, { before: new Date(), states: ['running'] });
  const schedule = await jobs(pool).schedule(a, { id: '1' }, { cron: '* * * * *' });
  const scheduledId: string = schedule.payload.id;
  void scheduledId;
  // @ts-expect-error Scheduling preserves the definition's payload contract.
  await jobs(pool).schedule(a, { id: 1 }, { cron: '* * * * *' });
  // @ts-expect-error Schedules have one catch-up/overlap contract.
  await jobs(pool).schedule(a, { id: '1' }, { cron: '* * * * *', missed: 'skip' });
  // @ts-expect-error Overlap is prevented by default, not a queue policy to select.
  await jobs(pool).schedule(a, { id: '1' }, { cron: '* * * * *', overlap: 'allow' });
  await runJobs(pool, [a, b]);
  // @ts-expect-error The application owns one pool for queue operations and execution.
  await runJobs('postgresql://localhost/example', [a, b]);
  // @ts-expect-error Pool creation is explicit.
  await runJobs({ connectionString: 'postgresql://localhost/example', maxConnections: 2 }, [a]);
  await runJobs(pool, [a], { retentionMs: false });
  await runJobs(pool, [a], { retentionMs: 30 * 86_400_000 });
  await runJobs(pool, [a], { notifications: 'postgresql://localhost/example' });
}
void types;

async function separated() {
  const pool = createPool({});
  const spec = defineJob<{ id: string }>({ name: 'shared', retryDelayMs: 100 });
  const worker = defineWorker(spec, {
    transactional: true,
    async handler(payload, { tx }) {
      await tx.queryClassic('SELECT $1::text', [payload.id]);
      await jobs(tx).send(spec, payload);
    },
  });
  await jobs(pool).send(spec, { id: 'one' });
  await jobs(pool).request(spec, { id: 'one' }, { key: 'one' });
  await jobs(pool).sendMany(spec, [{ payload: { id: 'one' } }]);
  // @ts-expect-error A producer contract preserves payload typing without a handler.
  await jobs(pool).send(spec, { id: 1 });
  // @ts-expect-error A worker cannot change the shared payload contract.
  defineWorker(spec, { handler(payload: number) {} });
  defineWorker(spec, {
    // @ts-expect-error A worker cannot require extra fields absent from the producer contract.
    handler(payload: { id: string; extra: string }) {
      void payload.extra;
    },
  });
  // @ts-expect-error A worker cannot override submission policy.
  defineWorker(spec, { handler(payload) {}, maxAttempts: 1 });
  // @ts-expect-error A producer contract cannot run without a bound handler.
  await runJobs(pool, [spec]);
  await runJobs(pool, [worker], {
    onEvent(event) {
      // @ts-expect-error Events do not carry job payload fields.
      void event.payload;
      if (event.type === 'attemptFailed') {
        const retry: Date | null = event.retryAt;
        void retry;
      }
    },
    async onHandlerError(failure): Promise<import('../src/index.js').HandlerErrorAction> {
      const original: unknown = failure.error;
      void original;
      await Promise.resolve();
      return failure.transactional ? 'stop' : 'continue';
    },
  });
  // @ts-expect-error The handler error policy must return a supported action.
  await runJobs(pool, [worker], { onHandlerError: () => 'retry' });
}
void separated;
