import { createPool } from '@kequtech/pgclear';
import { defineJob, runJobs } from '../../src/index.js';

const pool = createPool({ connectionString: process.env.TENON_JOBS_TEST_DATABASE_URL! });
const definition = defineJob({
  name: process.argv[2]!,
  retryDelayMs: 0,
  handler(_payload: number, context) {
    process.send?.(context.id);
    return new Promise<void>(() => {});
  },
});
await runJobs(pool, [definition], { leaseMs: 500, pollIntervalMs: 10 });
