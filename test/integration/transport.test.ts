import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import test from 'node:test';
import { createPool, DriverError } from '@kequtech/pgclear';
import { jobs, runJobs } from '../../src/index.js';
import { claimJob, finishClaim, recoverClaims } from '../../src/claims.js';
import type { Claim } from '../../src/claims.js';
import { connectionProxy } from '../helpers/connection-proxy.js';
import { definition, execute, pool, until, url } from '../helpers/database.js';

const within = { timeout: 15_000 };
const observe = (work: Promise<unknown>) =>
  work.then(
    () => null,
    (error: unknown) => error,
  );

async function expired(id: string) {
  await until(async () => {
    const { rows } = await pool.queryClassic<{ expired: boolean }>(
      'SELECT lease_until <= clock_timestamp() AS expired FROM pgclear_jobs.jobs WHERE id = $1',
      [id],
    );
    return rows[0]?.expired === true;
  });
}

async function listenerReady(name: string) {
  await until(async () => {
    const { rows } = await pool.queryClassic<{ ready: boolean }>(
      `SELECT EXISTS (SELECT FROM pg_stat_activity WHERE application_name = $1
        AND state = 'idle' AND query LIKE 'LISTEN %') AS ready`,
      [name],
    );
    return rows[0]?.ready === true;
  });
}

test('a lost autocommit response reconciles to one retained stable-ID job', within, async () => {
  const def = definition();
  const id = randomUUID();
  const proxy = await connectionProxy(url);
  const client = createPool({ ...proxy.connection, maxConnections: 1 });
  try {
    await client.queryClassic('SELECT 1');
    proxy.hold('responses');
    const outcome = observe(jobs(client).send(def, 7, { id }));
    await until(async () => (await jobs(pool).get(id))?.payload === 7);
    await until(async () => proxy.buffered('responses') > 0);
    proxy.disconnect();
    const error = await outcome;
    assert.ok(error instanceof DriverError);
    assert.equal(error.outcome, 'unknown');
    proxy.release();
    const reconciled = await jobs(client).send(def, 99, { id });
    assert.equal(reconciled.id, id);
    assert.equal(reconciled.payload, 7);
    const claimed = (await claimJob(execute, [def.name], 30_000)).claim!;
    await finishClaim(execute, claimed, null);
    const completed = await jobs(client).send(def, 100, { id });
    assert.equal(completed.state, 'completed');
    assert.equal(completed.payload, 7);
    assert.equal((await jobs(pool).stats(def)).total, 1);
    assert.equal((await jobs(pool).attempts(id)).length, 1);
  } finally {
    await proxy.close();
    await client.close();
  }
});

for (const commitDelivered of [false, true]) {
  test(
    `pgclear reconciles an uncertain transaction (COMMIT delivered: ${commitDelivered})`,
    within,
    async () => {
      const def = definition();
      const id = randomUUID();
      // An application-owned table, outside the library's reserved schema.
      const table = `public.${def.name.replaceAll('-', '_')}`;
      const proxy = await connectionProxy(url);
      const client = createPool({
        ...proxy.connection,
        maxConnections: 1,
        applicationName: def.name,
      });
      let callbacks = 0;
      const arm = () => {
        proxy.hold(commitDelivered ? 'responses' : 'requests');
      };
      try {
        await pool.queryClassic(
          `CREATE TABLE ${table} (id uuid PRIMARY KEY, value integer NOT NULL)`,
        );
        const transaction = client.transaction(async (tx) => {
          callbacks++;
          await tx.queryClassic(`INSERT INTO ${table} VALUES ($1, 7)`, [id]);
          await jobs(tx).send(def, 7, { id });
          arm();
        });
        const outcome = observe(transaction);
        await until(async () => proxy.buffered(commitDelivered ? 'responses' : 'requests') > 0);
        if (commitDelivered) await until(async () => (await jobs(pool).get(id))?.payload === 7);
        proxy.disconnect();
        const error = await outcome;
        assert.ok(error instanceof DriverError);
        assert.equal(error.code, 'PGCLEAR_COMMIT_UNKNOWN');
        assert.equal(error.outcome, 'unknown');
        assert.equal(callbacks, 1); // The library must not replay the application transaction.
        await until(async () => {
          const { rows } = await pool.queryClassic<{ closed: boolean }>(
            'SELECT NOT EXISTS (SELECT FROM pg_stat_activity WHERE application_name = $1) AS closed',
            [def.name],
          );
          return rows[0]?.closed === true;
        });
        const app = await pool.queryClassic<{ value: number }>(
          `SELECT value FROM ${table} WHERE id = $1`,
          [id],
        );
        assert.equal(app.rows.length, commitDelivered ? 1 : 0);
        assert.equal(app.rows[0]?.value ?? null, commitDelivered ? 7 : null);
        assert.equal((await jobs(pool).get(id))?.payload ?? null, commitDelivered ? 7 : null);
        proxy.release();
        if (!commitDelivered) {
          // Absence is authoritative only after the old session has ended and rolled back.
          await pool.transaction(async (tx) => {
            await tx.queryClassic(`INSERT INTO ${table} VALUES ($1, 7)`, [id]);
            await jobs(tx).send(def, 7, { id });
          });
        }
        assert.equal((await jobs(client).send(def, 99, { id })).payload, 7);
        assert.equal((await pool.queryClassic(`SELECT * FROM ${table}`)).rows.length, 1);
        assert.equal((await jobs(pool).stats(def)).total, 1);
      } finally {
        await proxy.close();
        await client.close();
        await pool.queryClassic(`DROP TABLE IF EXISTS ${table}`);
      }
    },
  );
}

for (const partition of ['both directions', 'responses only'] as const) {
  test(`${partition} partition cancels a handler at its last confirmed lease`, within, async () => {
    const proxy = await connectionProxy(url);
    const client = createPool({ ...proxy.connection, maxConnections: 1 });
    let signal: AbortSignal | undefined;
    const def = definition({
      maxAttempts: 2,
      executionTimeoutMs: 10_000,
      async handler(_payload, context) {
        signal = context.signal;
        await new Promise<void>((resolve) =>
          signal!.addEventListener('abort', () => resolve(), { once: true }),
        );
      },
    });
    const job = await jobs(pool).send(def, 1);
    const stop = new AbortController();
    let settled = false;
    const outcome = observe(
      runJobs(client, [def], {
        signal: stop.signal,
        leaseMs: 600,
        databaseTimeoutMs: 5000,
        recoveryIntervalMs: 30_000,
        pollIntervalMs: 10,
        shutdownTimeoutMs: 100,
      }),
    ).then((error) => {
      settled = true;
      return error;
    });
    try {
      await until(async () => signal !== undefined);
      await until(async () => client.stats.idle === 1 && client.stats.waiting === 0);
      const { rows } = await pool.queryClassic<{ record: Claim; lease: string }>(
        'SELECT to_jsonb(j) AS record, to_jsonb(lease_until) AS lease FROM pgclear_jobs.jobs j WHERE id = $1',
        [job.id],
      );
      const original = rows[0]!.record;
      proxy.hold('responses', ...(partition === 'both directions' ? (['requests'] as const) : []));
      if (partition === 'responses only') {
        // The DB renews ownership, but that renewal must not count until the worker receives it.
        await until(async () => {
          const renewed = await pool.queryClassic<{ extended: boolean }>(
            'SELECT lease_until > $2::timestamptz AS extended FROM pgclear_jobs.jobs WHERE id = $1',
            [job.id, rows[0]!.lease],
          );
          return renewed.rows[0]?.extended === true;
        });
      } else {
        await until(async () => proxy.buffered('requests') > 0);
      }
      await until(async () => signal!.aborted, 1500);
      assert.equal((signal!.reason as Error).message, 'Job lease expired');
      assert.equal(settled, false); // Cancellation did not depend on the driver's longer timeout.
      await expired(job.id);
      await recoverClaims(execute);
      const replacement = (await claimJob(execute, [def.name], 30_000)).claim!;
      assert.equal(replacement.id, job.id);
      assert.notEqual(replacement.claim_token, original.claim_token);
      assert.equal(await finishClaim(execute, original, null), false);
      assert.equal(await finishClaim(execute, replacement, null), true);
      stop.abort();
      proxy.release(); // Deliver the old renewal request/reply only after a new claim has completed.
      assert.equal(await outcome, null);
      assert.equal((await jobs(pool).get(job.id))?.state, 'completed');
      assert.deepEqual(
        (await jobs(pool).attempts(job.id)).map((attempt) => attempt.outcome),
        ['completed', 'expired'],
      );
    } finally {
      stop.abort();
      await proxy.close();
      await outcome;
      await client.close();
    }
  });
}

test(
  'an unhealed partition rejects the runner and leaves its abandoned claim recoverable',
  within,
  async () => {
    const proxy = await connectionProxy(url);
    const client = createPool({ ...proxy.connection, maxConnections: 1 });
    let signal: AbortSignal | undefined;
    const def = definition({
      maxAttempts: 2,
      executionTimeoutMs: 10_000,
      async handler(_payload, context) {
        signal = context.signal;
        await new Promise<void>((resolve) =>
          signal!.addEventListener('abort', () => resolve(), { once: true }),
        );
      },
    });
    const job = await jobs(pool).send(def, 1);
    const outcome = observe(
      runJobs(client, [def], {
        leaseMs: 300,
        databaseTimeoutMs: 600,
        recoveryIntervalMs: 30_000,
      }),
    );
    try {
      await until(async () => signal !== undefined);
      await until(async () => client.stats.idle === 1 && client.stats.waiting === 0);
      proxy.hold('requests', 'responses');
      await until(async () => signal!.aborted, 1000);
      assert.equal((signal!.reason as Error).message, 'Job lease expired');
      const error = await outcome;
      assert.ok(error instanceof DriverError);
      assert.equal(error.code, 'PGCLEAR_TIMEOUT');
      assert.equal((await jobs(pool).get(job.id))?.state, 'running');
      await recoverClaims(execute);
      const replacement = (await claimJob(execute, [def.name], 30_000)).claim!;
      assert.equal(replacement.id, job.id);
      assert.equal(replacement.attempt_count, 2);
      await finishClaim(execute, replacement, null);
      assert.equal((await jobs(pool).get(job.id))?.state, 'completed');
    } finally {
      await proxy.close();
      await outcome;
      await client.close();
    }
  },
);

test(
  'a claim response held beyond its lease never starts a handler after transport recovers',
  within,
  async () => {
    const proxy = await connectionProxy(url);
    const client = createPool({ ...proxy.connection, maxConnections: 1 });
    const handled: number[] = [];
    const def = definition({
      maxAttempts: 1,
      handler(value) {
        handled.push(value);
      },
    });
    const listenerName = `${def.name}-listener`;
    const stop = new AbortController();
    const outcome = observe(
      runJobs(client, [def], {
        signal: stop.signal,
        leaseMs: 300,
        databaseTimeoutMs: 5000,
        recoveryIntervalMs: 30_000,
        pollIntervalMs: 30_000,
        notifications: { connectionString: url, applicationName: listenerName },
      }),
    );
    try {
      await listenerReady(listenerName);
      await until(async () => client.stats.idle === 1 && client.stats.waiting === 0);
      proxy.hold('responses');
      const stranded = await jobs(pool).send(def, 1);
      await until(async () => (await jobs(pool).get(stranded.id))?.state === 'running');
      await expired(stranded.id);
      await recoverClaims(execute);
      assert.equal((await jobs(pool).get(stranded.id))?.state, 'failed');
      proxy.release();
      const fresh = await jobs(pool).send(def, 2);
      await until(async () => (await jobs(pool).get(fresh.id))?.state === 'completed');
      assert.deepEqual(handled, [2]);
      assert.deepEqual(
        (await jobs(pool).attempts(stranded.id)).map((attempt) => attempt.outcome),
        ['expired'],
      );
      stop.abort();
      assert.equal(await outcome, null);
    } finally {
      stop.abort();
      await proxy.close();
      await outcome;
      await client.close();
    }
  },
);

test(
  'a lost completion response rejects the runner without replaying a committed completion',
  within,
  async () => {
    const proxy = await connectionProxy(url);
    const client = createPool({ ...proxy.connection, maxConnections: 1 });
    const finish = Promise.withResolvers<void>();
    const handled: number[] = [];
    const def = definition({
      handler: async (value) => {
        handled.push(value);
        if (value === 1) await finish.promise;
      },
    });
    const job = await jobs(pool).send(def, 1);
    const outcome = observe(
      runJobs(client, [def], {
        leaseMs: 10_000,
        pollIntervalMs: 10,
        recoveryIntervalMs: 30_000,
        databaseTimeoutMs: 5000,
      }),
    );
    try {
      await until(async () => handled.length === 1);
      await until(async () => client.stats.idle === 1 && client.stats.waiting === 0);
      proxy.hold('responses');
      finish.resolve();
      await until(async () => (await jobs(pool).get(job.id))?.state === 'completed');
      proxy.disconnect();
      const error = await outcome;
      assert.ok(error instanceof DriverError);
      assert.equal(error.outcome, 'unknown');
      assert.equal((await jobs(pool).attempts(job.id)).length, 1);
      const stop = new AbortController();
      const marker = await jobs(pool).send(def, 2);
      const restarted = observe(runJobs(pool, [def], { signal: stop.signal, pollIntervalMs: 10 }));
      try {
        await until(async () => (await jobs(pool).get(marker.id))?.state === 'completed');
        assert.deepEqual(handled, [1, 2]);
      } finally {
        stop.abort();
        assert.equal(await restarted, null);
      }
    } finally {
      finish.resolve();
      await proxy.close();
      await outcome;
      await client.close();
    }
  },
);
