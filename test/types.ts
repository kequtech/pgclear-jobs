import { createPool } from '@kequtech/pgclear';
import type { QueryClient } from '@kequtech/tenon-orm';
import { defineJob, jobs, runJobs } from '../src/index.js';
import { sql as tenonSql } from '@kequtech/tenon-orm';
import { defineJob as defineTenonJob, jobs as tenonJobs } from '../src/tenon.js';

// Compile-time consumer contract; never executed.
async function types(tx: QueryClient) {
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
  const tenonTransactional = defineTenonJob({
    name: 'tenon-transactional',
    transactional: true,
    async handler(payload: { id: string }, { tx }) {
      const row = await tx.query(tenonSql<{ id: string }>`SELECT ${payload.id}::text AS id`, {
        returns: 'one',
      });
      const id: string = row.id;
      await tenonJobs(tx).send(a, { id });
      // @ts-expect-error A Tenon handler receives the Tenon API, not a native driver scope.
      await tx.queryClassic('SELECT 1');
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
  await tenonJobs(tx).send(tenonTransactional, { id: 'two' });
  // @ts-expect-error Transactional payload types remain bound to their definition.
  await jobs(pool).send(transactional, { id: 1 });
  await runJobs(pool, [a, transactional, tenonTransactional]);
  const job = await jobs(pool).send(a, { id: '1' });
  const id: string = job.payload.id;
  void id;
  // @ts-expect-error A payload cannot widen the definition's input type.
  await jobs(pool).send(a, { id: 3 });
  // @ts-expect-error Typed updates use the definition's payload.
  await jobs(pool).update(a, job.id, { id: false });
  await tenonJobs(tx).send(a, { id: '1' });
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
  await tenonJobs(tx).request(a, { id: 'latest' }, { key: 'one', delayMs: 100 });
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
  await tenonJobs(tx).schedule(a, { id: '1' }, { cron: '* * * * *' });
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
