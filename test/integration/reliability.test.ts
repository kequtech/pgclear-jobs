import assert from 'node:assert/strict';
import test from 'node:test';
import { defineJob, jobs, runJobs } from '../../src/index.js';
import { setTimeout as delay } from 'node:timers/promises';
import type { InputValue, QueryOptions, QueryResult } from '@kequtech/pgclear';
import { claimJob, finishClaim, renewClaim } from '../../src/claims.js';
import { definition, execute, pool, until } from '../helpers/database.js';
import type { Execute } from '../../src/storage.js';

function delayResponses(afterQuery: (rows: readonly object[]) => Promise<void>) {
  return new Proxy(pool, {
    get(target, key) {
      if (key === 'queryClassic')
        return async <Row extends object>(
          text: string,
          values?: readonly InputValue[],
          options?: QueryOptions,
        ): Promise<QueryResult<Row>> => {
          const result = await pool.queryClassic<Row>(text, values, options);
          await afterQuery(result.rows);
          return result;
        };
      const value: unknown = Reflect.get(target, key, target);
      return typeof value === 'function' ? value.bind(target) : value;
    },
  });
}

for (const operation of ['complete', 'renew'] as const) {
  for (const expiry of ['lease_until', 'deadline'] as const) {
    test(`${operation} rechecks ${expiry} after waiting for a row lock`, async () => {
      const def = definition();
      const job = await jobs(pool).send(def, 1);
      const claim = (await claimJob(execute, [def.name], 30_000)).claim!;
      await pool.queryClassic(
        `UPDATE pgclear_jobs.jobs SET ${expiry} = clock_timestamp() + interval '500 milliseconds' WHERE id = $1`,
        [job.id],
      );
      const session = await pool.acquire();
      const contender: Execute = (text, values) => session.queryClassic(text, values);
      let pending: Promise<boolean | 'cancelled'> | undefined;
      try {
        await pool.transaction(async (tx) => {
          await tx.queryClassic('SELECT id FROM pgclear_jobs.jobs WHERE id = $1 FOR UPDATE', [
            job.id,
          ]);
          pending =
            operation === 'complete'
              ? finishClaim(contender, claim, null)
              : renewClaim(contender, claim, 30_000);
          await until(async () => {
            const result = await pool.queryClassic<{ blocked: boolean }>(
              'SELECT cardinality(pg_blocking_pids($1)) > 0 AS blocked',
              [session.info.backendPid],
            );
            return result.rows[0]?.blocked === true;
          });
          await until(async () => {
            const result = await pool.queryClassic<{ expired: boolean }>(
              `SELECT ${expiry} <= clock_timestamp() AS expired FROM pgclear_jobs.jobs WHERE id = $1`,
              [job.id],
            );
            return result.rows[0]?.expired === true;
          });
        });
        assert.equal(await pending, false);
        assert.equal((await jobs(pool).get(job.id))?.state, 'running');
      } finally {
        await pending;
        await session.release();
      }
    });
  }
}

for (const budget of ['deadline', 'lease'] as const) {
  test(`a delayed claim response cannot start a handler after its ${budget}`, async () => {
    let called = 0;
    let delivered = false;
    const def = definition({
      maxAttempts: 1,
      executionTimeoutMs: budget === 'deadline' ? 60 : 2000,
      handler() {
        called++;
      },
    });
    const job = await jobs(pool).send(def, 1);
    const delayed = delayResponses(async (rows) => {
      const row = rows[0] as { record?: { claim_token?: string } } | undefined;
      if (row?.record?.claim_token) {
        await delay(150);
        delivered = true;
      }
    });
    const stop = new AbortController();
    const running = runJobs(delayed, [def], {
      signal: stop.signal,
      leaseMs: budget === 'lease' ? 60 : 30_000,
      recoveryIntervalMs: 20,
      shutdownTimeoutMs: 20,
    });
    try {
      await until(async () => delivered);
      await delay(20);
      await until(async () => (await jobs(pool).get(job.id))?.state === 'failed');
    } finally {
      stop.abort();
      await running;
    }
    assert.equal(called, 0);
  });
}

test('a delayed heartbeat response cannot extend the locally confirmed lease', async () => {
  let aborted = false;
  let renewing = false;
  const response = Promise.withResolvers<void>();
  const def = definition({
    executionTimeoutMs: 2000,
    async handler(_payload, { signal }) {
      await new Promise<void>((resolve) => {
        signal.addEventListener(
          'abort',
          () => {
            aborted = true;
            resolve();
          },
          { once: true },
        );
      });
    },
  });
  await jobs(pool).send(def, 1);
  const delayed = delayResponses(async (rows) => {
    if ('id' in (rows[0] ?? {})) {
      renewing = true;
      await response.promise;
    }
  });
  const stop = new AbortController();
  const running = runJobs(delayed, [def], {
    signal: stop.signal,
    leaseMs: 150,
    recoveryIntervalMs: 2000,
    shutdownTimeoutMs: 20,
  });
  try {
    await until(async () => renewing);
    await until(async () => aborted, 800);
  } finally {
    response.resolve();
    stop.abort();
    await running;
  }
});

test('invalid PostgreSQL JSON text is rejected before it aborts the caller transaction', async () => {
  const def = defineJob({ name: definition().name, handler(_payload: unknown) {} });
  await pool.transaction(async (tx) => {
    for (const payload of [{ value: '\u0000' }, { value: '\ud800' }, { ['\u0000']: 1 }]) {
      await assert.rejects(jobs(tx).send(def, payload), TypeError);
    }
    for (const key of ['\0', '\ud800']) {
      assert.throws(() => defineJob({ name: key, handler() {} }), TypeError);
      await assert.rejects(jobs(tx).request(def, null, { key }), TypeError);
      await assert.rejects(jobs(tx).find(def, { key }), TypeError);
      await assert.rejects(jobs(tx).schedule(def, null, { cron: '* * * * *', key }), TypeError);
      await assert.rejects(jobs(tx).unschedule(def, key), TypeError);
    }
    await tx.queryClassic('SELECT 1');
  });
});

test('a failure message containing a NUL character remains a normal inspectable failure', async () => {
  const def = definition({
    maxAttempts: 1,
    handler() {
      throw new Error('invalid\u0000message');
    },
  });
  const job = await jobs(pool).send(def, 1);
  const stop = new AbortController();
  const running = runJobs(pool, [def], { signal: stop.signal, shutdownTimeoutMs: 20 });
  // Observe early runner rejection so a serialization regression remains an assertion failure.
  const outcome = running.then(
    () => null,
    (error: unknown) => error,
  );
  try {
    await Promise.race([
      until(async () => (await jobs(pool).get(job.id))?.state === 'failed'),
      outcome.then((error) => {
        if (error) throw error;
      }),
    ]);
    assert.ok((await jobs(pool).get(job.id))?.lastError?.includes('invalid'));
  } finally {
    stop.abort();
    await outcome;
  }
});
