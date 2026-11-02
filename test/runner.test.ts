import assert from 'node:assert/strict';
import test from 'node:test';
import { createPool } from '@kequtech/pgclear';
import { defineJob, runJobs } from '../src/index.js';
import { createJobsIntegration } from '../src/integration.js';

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
    await assert.rejects(
      runJobs(pool, [def], {
        signal: AbortSignal.abort(),
        // @ts-expect-error Readiness validates before pool access, even when pre-aborted.
        onReady: true,
      }),
      TypeError,
    );
    const stop = AbortSignal.abort();
    await runJobs(pool, [def], { signal: stop });
    await runJobs(pool, [def], { signal: stop, retentionMs: false });
    await runJobs(pool, [def], { signal: stop, retentionMs: 30 * 86_400_000 });
    assert.equal(pool.stats.total, 0);
  } finally {
    await pool.close();
  }
});

test('storage validation failures preserve their error and never announce readiness', async (t) => {
  const pool = createPool({});
  const failure = new Error('storage validation failed');
  let announced = false;
  t.mock.method(pool, 'queryClassic', async () => {
    throw failure;
  });
  try {
    await assert.rejects(
      runJobs(pool, [defineJob({ name: 'validation', handler() {} })], {
        onReady() {
          announced = true;
        },
      }),
      (error) => error === failure,
    );
    assert.equal(announced, false);
    assert.equal(pool.stats.total, 0);
  } finally {
    await pool.close();
  }
});

test('an integration is inert and a pre-aborted runner does not resolve its pool', async () => {
  const unexpected = (): never => {
    throw new Error('unexpected integration operation');
  };
  const api = createJobsIntegration<object, { readonly custom: true }>({
    pool: unexpected,
    execute: unexpected,
    transaction: async () => unexpected(),
  });
  const def = api.defineJob({
    name: 'inert',
    transactional: true,
    handler(_payload: number, { tx }) {
      void tx.custom;
    },
  });
  await api.runJobs({}, [def], { signal: AbortSignal.abort(), onReady: unexpected });
  await assert.rejects(api.runJobs({}, [def], { concurrency: 0 }), TypeError);
  await assert.rejects(
    api.runJobs({}, [def], {
      // @ts-expect-error Invalid readiness must fail before resolving an integration's pool.
      onReady: null,
    }),
    TypeError,
  );
});

test('handlerless or malformed worker registrations and hooks fail before pool access', async () => {
  const unexpected = (): never => {
    throw new Error('pool access was not expected');
  };
  const api = createJobsIntegration<object, object>({
    pool: unexpected,
    execute: unexpected,
    transaction: async () => unexpected(),
  });
  const spec = api.defineJob<number>({ name: 'producer-only' });
  // @ts-expect-error Producers are not executable worker registrations.
  await assert.rejects(api.runJobs({}, [spec]), TypeError);
  const worker = api.defineWorker(spec, { handler(_payload) {} });
  for (const options of [{ onEvent: true }, { onHandlerError: false }]) {
    await assert.rejects(api.runJobs({}, [worker], options as never), TypeError);
  }
  for (const definition of [
    { ...worker, transactional: 'yes' },
    { ...worker, concurrency: 0 },
    { ...worker, name: '\0' },
  ]) {
    await assert.rejects(api.runJobs({}, [definition] as never), TypeError);
  }
});
