import assert from 'node:assert/strict';
import { setTimeout as delay } from 'node:timers/promises';
import test from 'node:test';
import { createPool } from '@kequtech/pgclear';
import { jobs, runJobs } from '../../src/index.js';
import { claimJob, finishClaim, recoverClaims } from '../../src/claims.js';
import { definition, execute, pool, until, url } from '../helpers/database.js';

const within = { timeout: 15_000 };

test(
  'locked definitions are skipped while each definition retains oldest-due-first claims',
  within,
  async () => {
    const busy = definition();
    const quiet = definition();
    const oldest = await jobs(pool).send(busy, 1, { runAt: new Date(0) });
    const newer = await jobs(pool).send(busy, 2, { runAt: new Date(1) });
    const ready = await jobs(pool).send(quiet, 3);
    await pool.transaction(async (tx) => {
      await tx.queryClassic('SELECT id FROM pgclear_jobs.jobs WHERE name = $1 FOR UPDATE', [
        busy.name,
      ]);
      const { claim } = await claimJob(
        (sql, values) => pool.queryClassic(sql, values, { timeoutMs: 1000 }),
        [busy.name, quiet.name],
        30_000,
      );
      assert.equal(claim?.id, ready.id);
      await finishClaim(execute, claim!, null);
    });
    const first = (await claimJob(execute, [busy.name], 30_000)).claim!;
    assert.equal(first.id, oldest.id);
    await finishClaim(execute, first, null);
    assert.equal((await claimJob(execute, [busy.name], 30_000)).claim?.id, newer.id);
  },
);

test('a quiet definition receives a turn before a much older backlog drains', within, async () => {
  const stop = new AbortController();
  let busyStarts = 0,
    quietStarts = 0;
  const busy = definition({
    handler() {
      if (++busyStarts === 5) stop.abort();
    },
  });
  const quiet = definition({
    handler() {
      quietStarts++;
      stop.abort();
    },
  });
  await jobs(pool).sendMany(
    busy,
    Array.from({ length: 200 }, () => ({ payload: 1, runAt: new Date(0) })),
  );
  await jobs(pool).send(quiet, 2);
  await runJobs(pool, [busy, quiet], { signal: stop.signal });
  assert.equal(quietStarts, 1);
  assert.equal(busyStarts, 1);
});

test(
  'an empty or future definition does not delay ready work in another definition',
  within,
  async () => {
    const stop = new AbortController();
    const handled: number[] = [];
    const empty = definition();
    const future = definition({
      handler(value) {
        handled.push(value);
      },
    });
    const ready = definition({
      handler(value) {
        handled.push(value);
        stop.abort();
      },
    });
    await jobs(pool).send(future, 1, { delayMs: 60_000 });
    const work = await jobs(pool).send(ready, 2);
    await runJobs(pool, [empty, future, ready], { signal: stop.signal });
    assert.deepEqual(handled, [2]);
    assert.equal((await jobs(pool).get(work.id))?.state, 'completed');
  },
);

test(
  'small shared pools process continuous submissions and retries alongside application traffic',
  within,
  async () => {
    const client = createPool({ connectionString: url, maxConnections: 2 });
    const stop = new AbortController();
    const failures: unknown[] = [];
    const calls = new Map<number, number>();
    let active = 0,
      maximum = 0,
      applicationDone = false,
      applicationCalls = 0;
    const def = definition({
      maxAttempts: 2,
      retryDelayMs: 5,
      async handler(value, { attempt, signal }) {
        active++;
        maximum = Math.max(maximum, active);
        calls.set(value, (calls.get(value) ?? 0) + 1);
        try {
          await client.queryClassic('SELECT pg_sleep(0.003)', [], { signal });
          await delay(3, undefined, { signal });
          if (value % 17 === 0 && attempt === 1) throw new Error('Expected retry');
        } finally {
          active--;
        }
      },
    });
    const runners = [0, 1].map(() =>
      runJobs(client, [def], {
        signal: stop.signal,
        concurrency: 4,
        leaseMs: 2000,
        pollIntervalMs: 5,
        recoveryIntervalMs: 50,
        shutdownTimeoutMs: 1000,
      }).catch((error: unknown) => {
        failures.push(error);
        stop.abort();
      }),
    );
    const application = (async () => {
      while (!applicationDone) {
        await client.transaction(async (tx) => {
          await tx.queryClassic('SELECT pg_sleep(0.005)');
        });
        applicationCalls++;
        await delay(5);
      }
    })().catch((error: unknown) => {
      failures.push(error);
    });
    try {
      for (let batch = 0; batch < 12; batch++) {
        await jobs(client).sendMany(
          def,
          Array.from({ length: 20 }, (_, offset) => ({ payload: batch * 20 + offset })),
        );
        await delay(10);
      }
      await until(async () => {
        if (failures.length) throw failures[0];
        return (await jobs(pool).stats(def)).completed === 240;
      }, 10_000);
      assert.ok(applicationCalls > 0);
      assert.ok(maximum > 2 && maximum <= 8);
      assert.equal(calls.size, 240);
      for (const [value, count] of calls) assert.equal(count, value % 17 === 0 ? 2 : 1);
      const { rows } = await pool.queryClassic<{ expired: number }>(
        `SELECT count(*)::integer AS expired FROM pgclear_jobs.attempts a JOIN pgclear_jobs.jobs j ON j.id = a.job_id
       WHERE j.name = $1 AND a.outcome = 'expired'`,
        [def.name],
      );
      assert.equal(rows[0]!.expired, 0);
    } finally {
      applicationDone = true;
      stop.abort();
      await Promise.all([...runners, application]);
      await client.close();
    }
    assert.deepEqual(failures, []);
  },
);

test(
  'exhausting the pool cancels a local lease and resumes after connections are released',
  within,
  async () => {
    const client = createPool({ connectionString: url, maxConnections: 1 });
    const stop = new AbortController();
    const handled: number[] = [];
    let signal: AbortSignal | undefined;
    const def = definition({
      maxAttempts: 1,
      async handler(value, context) {
        handled.push(value);
        if (value === 1) {
          signal = context.signal;
          await new Promise<void>((resolve) =>
            signal!.addEventListener('abort', () => resolve(), { once: true }),
          );
        }
      },
    });
    const first = await jobs(pool).send(def, 1);
    const running = runJobs(client, [def], {
      signal: stop.signal,
      leaseMs: 400,
      databaseTimeoutMs: 3000,
      recoveryIntervalMs: 30_000,
      pollIntervalMs: 10,
      shutdownTimeoutMs: 100,
    });
    const outcome = running.then(
      () => null,
      (error: unknown) => error,
    );
    let lease: Awaited<ReturnType<typeof client.acquire>> | undefined;
    try {
      await until(async () => signal !== undefined);
      lease = await client.acquire();
      const second = await jobs(pool).send(def, 2);
      await until(async () => client.stats.waiting > 0);
      await until(async () => signal!.aborted, 1000);
      assert.equal((signal!.reason as Error).message, 'Job lease expired');
      await until(async () => {
        const { rows } = await pool.queryClassic<{ expired: boolean }>(
          'SELECT lease_until <= clock_timestamp() AS expired FROM pgclear_jobs.jobs WHERE id = $1',
          [first.id],
        );
        return rows[0]!.expired;
      });
      await recoverClaims(execute);
      assert.equal((await jobs(pool).get(first.id))?.state, 'failed');
      await lease.release();
      lease = undefined;
      await until(async () => (await jobs(pool).get(second.id))?.state === 'completed');
      assert.deepEqual(handled, [1, 2]);
    } finally {
      await lease?.release();
      stop.abort();
      await outcome;
      await client.close();
    }
    assert.equal(await outcome, null);
  },
);
