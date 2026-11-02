import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { randomUUID } from 'node:crypto';
import test from 'node:test';
import { createPool } from '@kequtech/pgclear';
import { defineWorker, jobs, startJobs } from '../../src/index.js';
import { definition, pool, until, url } from '../helpers/database.js';

test('the runtime role executes queue workflows without storage ownership, DDL or PUBLIC execution', async () => {
  const role = `jobs_runtime_${randomUUID().replaceAll('-', '')}`;
  await pool.queryClassic(`CREATE ROLE ${role} NOLOGIN`);
  const runtime = createPool({
    connectionString: url,
    startupParameters: { role },
    maxConnections: 4,
  });
  const def = definition({ maxAttempts: 1 });
  const queue = jobs(runtime);
  const stop = new AbortController();
  try {
    const session = await pool.acquire();
    try {
      await session.script(
        (await readFile('examples/runtime-grants.sql', 'utf8')).replaceAll('jobs_runtime', role),
      );
    } finally {
      await session.release();
    }
    assert.equal(
      (await runtime.queryClassic<{ name: string }>('SELECT current_user AS name')).rows[0]!.name,
      role,
    );
    await assert.rejects(runtime.queryClassic('CREATE TABLE pgclear_jobs.forbidden(id integer)'), {
      code: '42501',
    });
    const first = await queue.request(def, 1, { key: 'grants' });
    const failed = await queue.send(def, 2);
    await queue.schedule(def, 3, { cron: '* * * * *', requestKey: 'grants' });
    const worker = defineWorker(def, {
      handler(payload) {
        if (payload === 2) throw new Error('permanent');
      },
    });
    const handle = startJobs(runtime, [worker], { signal: stop.signal, pollIntervalMs: 10 });
    try {
      await handle.ready;
      await until(
        async () =>
          (await queue.get(first.id))?.state === 'completed' &&
          (await queue.get(failed.id))?.state === 'failed',
      );
      const retried = await queue.retry(failed.id);
      await until(async () => (await queue.get(retried.id))?.state === 'failed');
    } finally {
      stop.abort();
      await handle.done;
    }
    assert.ok((await queue.attempts(first.id)).length);
    await queue.unschedule(def);
    assert.equal(
      await queue.prune(def, {
        before: new Date(Date.now() + 1000),
        states: ['completed', 'failed'],
      }),
      3,
    );
    const acl = await pool.queryClassic<{ allowed: boolean }>(`SELECT EXISTS (
      SELECT FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace,
      LATERAL aclexplode(COALESCE(p.proacl, acldefault('f',p.proowner))) a
      WHERE n.nspname='pgclear_jobs' AND a.grantee=0 AND a.privilege_type='EXECUTE'
    ) AS allowed`);
    assert.equal(acl.rows[0]!.allowed, false);
  } finally {
    stop.abort();
    await runtime.close();
    await pool.queryClassic(`DROP OWNED BY ${role}`);
    await pool.queryClassic(`DROP ROLE ${role}`);
  }
});
