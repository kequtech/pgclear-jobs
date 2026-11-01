import { randomUUID } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { before, after } from 'node:test';
import { createPool } from '@kequtech/pgclear';
import { defineJob, installJobs } from '../../src/index.js';
import type { JobOptions } from '../../src/index.js';
import type { Execute } from '../../src/storage.js';

const configured = process.env.PGCLEAR_JOBS_TEST_DATABASE_URL;
if (!configured || !new URL(configured).pathname.endsWith('_test')) {
  throw new Error('Set PGCLEAR_JOBS_TEST_DATABASE_URL to a dedicated database ending in _test.');
}
export const url = configured;
export const pool = createPool({ connectionString: url, maxConnections: 12 });
export const execute: Execute = (text, values) => pool.queryClassic(text, values);
export const names: string[] = [];
export function definition(
  options: Partial<Extract<JobOptions<number>, { transactional?: false }>> = {},
) {
  const name = `test-${randomUUID()}`;
  names.push(name);
  return defineJob({
    name,
    retryDelayMs: 0,
    retryJitter: false,
    handler(_payload: number) {},
    ...options,
  });
}
export async function until(check: () => Promise<boolean>, timeout = 5000) {
  const deadline = performance.now() + timeout;
  while (!(await check())) {
    if (performance.now() > deadline)
      throw new Error('Timed out waiting for observable job state.');
    await delay(10);
  }
}

before(async () => {
  await Promise.all([installJobs(pool), installJobs(pool)]);
});
after(async () => {
  try {
    await pool.queryClassic(
      'DELETE FROM pgclear_jobs.schedules WHERE name IN (SELECT jsonb_array_elements_text($1::jsonb))',
      [JSON.stringify(names)],
    );
    await pool.queryClassic(
      'DELETE FROM pgclear_jobs.jobs WHERE name IN (SELECT jsonb_array_elements_text($1::jsonb))',
      [JSON.stringify(names)],
    );
  } finally {
    await pool.close();
  }
});
