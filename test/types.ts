import { createPool } from '@kequtech/pgclear';
import type { QueryClient } from '@kequtech/tenon-orm';
import { defineJob, jobs, runJobs } from '../src/index.js';
import { jobs as tenonJobs } from '../src/tenon.js';

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
  await jobs(pool).upsert(a, { id: '1' }, { key: 'one' });
  // @ts-expect-error Upserts require an outstanding key.
  await jobs(pool).upsert(a, { id: '1' }, {});
  // @ts-expect-error Upserts preserve the definition's input type.
  await jobs(pool).upsert(a, { id: false }, { key: 'one' });
  await jobs(pool).find(a, { after: job.cursor });
  // @ts-expect-error Cleanup only accepts terminal states.
  await jobs(pool).prune(a, { before: new Date(), states: ['running'] });
  const schedule = await jobs(pool).schedule(a, { id: '1' }, { cron: '* * * * *' });
  const scheduledId: string = schedule.payload.id;
  void scheduledId;
  // @ts-expect-error Scheduling preserves the definition's payload contract.
  await jobs(pool).schedule(a, { id: 1 }, { cron: '* * * * *' });
  await tenonJobs(tx).schedule(a, { id: '1' }, { cron: '* * * * *' });
  await runJobs(pool, [a, b]);
  await runJobs('postgresql://localhost/example', [a, b]);
  await runJobs({ connectionString: 'postgresql://localhost/example', maxConnections: 2 }, [a]);
  await runJobs(pool, [a], { notifications: 'postgresql://localhost/example' });
}
void types;
