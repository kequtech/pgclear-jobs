import assert from 'node:assert/strict';
import test from 'node:test';
import { createPool } from '@kequtech/pgclear';
import { jobs, defineJob, startJobs } from '../src/index.js';
import { createJobsIntegration } from '../src/integration.js';

test('empty and invalid bulk lookups execute no SQL', async () => {
  let statements = 0;
  const records = jobs({
    async queryClassic() {
      statements++;
      throw new Error('unexpected SQL');
    },
  });
  assert.deepEqual(await records.getMany([]), []);
  for (const ids of [['invalid'], new Array<string>(1), Array<string>(1001).fill('invalid')])
    await assert.rejects(records.getMany(ids), TypeError);
  assert.equal(statements, 0);
});

test('pre-aborted handles settle both promises without resolving a pool', async () => {
  const api = createJobsIntegration<object, object>({
    pool() {
      throw new Error('unexpected pool access');
    },
    execute() {
      throw new Error('unexpected execution');
    },
    async transaction() {
      throw new Error('unexpected transaction');
    },
  });
  const worker = api.defineJob({ name: 'pre-aborted', handler() {} });
  const handle = api.startJobs({}, [worker], { signal: AbortSignal.abort() });
  await assert.rejects(handle.ready, { name: 'AbortError' });
  await handle.done;
});

test('startup handles preserve original validation and database errors on both promises', async (t) => {
  const pool = createPool({});
  const failure = new Error('storage unavailable');
  t.mock.method(pool, 'queryClassic', async () => {
    throw failure;
  });
  const worker = defineJob({ name: 'startup', handler() {} });
  try {
    const handle = startJobs(pool, [worker]);
    await assert.rejects(handle.ready, (e) => e === failure);
    await assert.rejects(handle.done, (e) => e === failure);
    const invalid = startJobs(pool, [worker], { concurrency: 0 });
    await assert.rejects(invalid.ready, TypeError);
    await assert.rejects(invalid.done, TypeError);
  } finally {
    await pool.close();
  }
});
