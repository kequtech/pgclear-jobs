import assert from 'node:assert/strict';
import test from 'node:test';
import { createPool } from '@kequtech/pgclear';
import { defineJob, runJobs } from '../src/index.js';

test('runner options validate before connecting and a pre-aborted runner opens no sessions', async () => {
  const pool = createPool({});
  const def = defineJob({ name: 'validation', handler() {} });
  try {
    // @ts-expect-error The caller supplies an existing pool.
    await assert.rejects(runJobs('postgresql://localhost/example', [def]), TypeError);
    for (const retentionMs of [0, -1, Infinity]) {
      await assert.rejects(runJobs(pool, [def], { retentionMs }), TypeError);
    }
    // @ts-expect-error Disable notifications by omitting the listener settings.
    await assert.rejects(runJobs(pool, [def], { notifications: false }), TypeError);
    const stop = AbortSignal.abort();
    await runJobs(pool, [def], { signal: stop });
    await runJobs(pool, [def], { signal: stop, retentionMs: false });
    await runJobs(pool, [def], { signal: stop, retentionMs: 30 * 86_400_000 });
    assert.equal(pool.stats.total, 0);
  } finally {
    await pool.close();
  }
});
