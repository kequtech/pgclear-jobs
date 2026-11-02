import { createPool } from '@kequtech/pgclear';
import { setTimeout as delay } from 'node:timers/promises';
import { defineJob, runJobs } from '../../src/index.js';

const pool = createPool(process.env.PGCLEAR_JOBS_TEST_DATABASE_URL!);
const stop = new AbortController();
process.once('SIGTERM', () => stop.abort());
const worker = defineJob({
  name: process.argv[2]!,
  async handler(_payload: number, { signal }) {
    process.send?.('started');
    try {
      await delay(60000, undefined, { signal });
    } finally {
      if (signal.aborted) process.send?.('aborted');
    }
  },
});
try {
  await runJobs(pool, [worker], { signal: stop.signal, leaseMs: 600, pollIntervalMs: 10 });
} finally {
  await pool.close();
  process.disconnect?.();
}
