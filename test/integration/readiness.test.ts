import assert from 'node:assert/strict';
import test from 'node:test';
import { setImmediate as nextTurn } from 'node:timers/promises';
import { createPool } from '@kequtech/pgclear';
import type { Pool, Transaction } from '@kequtech/pgclear';
import { createJobsIntegration } from '../../src/integration.js';
import { defineJob, jobs, runJobs } from '../../src/index.js';
import { definition, pool, until, url } from '../helpers/database.js';

const integrated = createJobsIntegration<Pool, Transaction>({
  pool: (pool) => pool,
  execute: (scope) => (sql, values) => scope.queryClassic(sql, values),
  transaction: (pool, work, options) => pool.transaction(work, options),
});

for (const [name, start] of Object.entries({ native: runJobs, integrated: integrated.runJobs })) {
  test(`${name} ready is called once after validation and awaited before dispatch`, async () => {
    const release = Promise.withResolvers<void>();
    const entered = Promise.withResolvers<void>();
    const def = defineJob({
      name: definition().name,
      handler(_payload: number) {
        calls++;
      },
    });
    const job = await jobs(pool).send(def, 1);
    let calls = 0;
    let readyCalls = 0;
    const stop = new AbortController();
    const running = start(pool, [def], {
      signal: stop.signal,
      pollIntervalMs: 10,
      async onReady() {
        readyCalls++;
        entered.resolve();
        await release.promise;
      },
    });
    void running.catch(() => {});
    try {
      await entered.promise;
      assert.equal(calls, 0);
      assert.equal((await jobs(pool).get(job.id))?.state, 'pending');
      release.resolve();
      await until(async () => (await jobs(pool).get(job.id))?.state === 'completed');
      assert.equal(readyCalls, 1);
      assert.equal(calls, 1);
    } finally {
      release.resolve();
      stop.abort();
      await running;
    }
    assert.equal((await pool.queryClassic('SELECT 1')).rows.length, 1);
  });
}

for (const asynchronous of [false, true]) {
  test(`a ${asynchronous ? 'rejected' : 'thrown'} ready callback fails startup with its original error`, async () => {
    const failure = new Error('host startup failed');
    const def = definition();
    const job = await jobs(pool).send(def, 1);
    await assert.rejects(
      runJobs(pool, [def], {
        onReady() {
          if (asynchronous) return Promise.reject(failure);
          throw failure;
        },
      }),
      (error) => error === failure,
    );
    assert.equal((await jobs(pool).get(job.id))?.state, 'pending');
  });
}

test('shutdown interrupts a pending ready callback and observes a later rejection', async () => {
  const entered = Promise.withResolvers<void>();
  const release = Promise.withResolvers<void>();
  const stop = new AbortController();
  const def = definition();
  const job = await jobs(pool).send(def, 1);
  const running = runJobs(pool, [def], {
    signal: stop.signal,
    onReady() {
      entered.resolve();
      return release.promise;
    },
  });
  void running.catch(() => {});
  try {
    await entered.promise;
    stop.abort();
    await running;
    assert.equal((await jobs(pool).get(job.id))?.state, 'pending');
    release.reject(new Error('late host error'));
    await nextTurn();
  } finally {
    stop.abort();
    release.resolve();
    await running;
  }
});

test('abort during storage validation does not announce ready or start dispatch', async (t) => {
  const client = createPool({ connectionString: url });
  const execute = client.queryClassic.bind(client);
  const entered = Promise.withResolvers<void>();
  const release = Promise.withResolvers<void>();
  let first = true;
  t.mock.method(client, 'queryClassic', async (...args: Parameters<typeof client.queryClassic>) => {
    if (first) {
      first = false;
      entered.resolve();
      await release.promise;
    }
    return execute(...args);
  });
  const stop = new AbortController();
  let ready = false;
  const running = runJobs(client, [definition()], {
    signal: stop.signal,
    onReady() {
      ready = true;
    },
  });
  void running.catch(() => {});
  try {
    await entered.promise;
    stop.abort();
    release.resolve();
    await running;
    assert.equal(ready, false);
  } finally {
    release.resolve();
    stop.abort();
    await running;
    await client.close();
  }
});

test('a synchronous ready error is preserved even if the callback requests shutdown', async () => {
  const stop = new AbortController();
  const failure = new Error('host startup failed during shutdown');
  const def = definition();
  const job = await jobs(pool).send(def, 1);
  await assert.rejects(
    runJobs(pool, [def], {
      signal: stop.signal,
      onReady() {
        stop.abort();
        throw failure;
      },
    }),
    (error) => error === failure,
  );
  assert.equal((await jobs(pool).get(job.id))?.state, 'pending');
});

for (const rejects of [false, true]) {
  test(`a ready callback can request shutdown before returning ${rejects ? 'a rejected promise' : 'normally'}`, async () => {
    const stop = new AbortController();
    const def = definition();
    const job = await jobs(pool).send(def, 1);
    let readyCalls = 0;
    await runJobs(pool, [def], {
      signal: stop.signal,
      onReady() {
        readyCalls++;
        stop.abort();
        if (rejects) return Promise.reject(new Error('late host error'));
      },
    });
    await nextTurn();
    assert.equal(readyCalls, 1);
    assert.equal((await jobs(pool).get(job.id))?.state, 'pending');
  });
}
