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
  await runJobs(pool, [a, b]);
}
void types;
