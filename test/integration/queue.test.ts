import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { fork } from 'node:child_process';
import { setTimeout as delay } from 'node:timers/promises';
import { once } from 'node:events';
import test, { after, before } from 'node:test';
import { createPool } from '@kequtech/pgclear';
import { createTenon, unsafeSql } from '@kequtech/tenon-orm';
import {
  defineJob,
  installJobs,
  jobs,
  JobNotFoundError,
  JobStateError,
  runJobs,
} from '../../src/index.js';
import { jobs as tenonJobs } from '../../src/tenon.js';
import { claimJob, finishClaim, recoverClaims, renewClaim } from '../../src/claims.js';
import type { Execute } from '../../src/storage.js';

const url = process.env.TENON_JOBS_TEST_DATABASE_URL;
if (!url || !new URL(url).pathname.endsWith('_test')) {
  throw new Error('Set TENON_JOBS_TEST_DATABASE_URL to a dedicated database ending in _test.');
}
const pool = createPool({ connectionString: url, maxConnections: 12 });
const execute: Execute = (text, values) => pool.queryClassic(text, values);
const names: string[] = [];
function definition(options: Partial<Parameters<typeof defineJob<number>>[0]> = {}) {
  const name = `test-${randomUUID()}`;
  names.push(name);
  return defineJob({ name, retryDelayMs: 0, handler(_payload: number) {}, ...options });
}
async function until(check: () => Promise<boolean>, timeout = 5000) {
  const deadline = Date.now() + timeout;
  while (!(await check())) {
    if (Date.now() > deadline) throw new Error('Timed out waiting for observable job state.');
    await delay(10);
  }
}

before(async () => {
  await Promise.all([installJobs(pool), installJobs(pool)]);
});
after(async () => {
  try {
    await pool.queryClassic(
      'DELETE FROM tenon_jobs.jobs WHERE name IN (SELECT jsonb_array_elements_text($1::jsonb))',
      [JSON.stringify(names)],
    );
  } finally {
    await pool.close();
  }
});

test('installation is repeatable and safe across concurrent installers', async () => {
  await Promise.all([installJobs(pool), installJobs(pool), installJobs(pool)]);
  const result = await pool.queryClassic<{ count: string }>(
    'SELECT count(*)::text AS count FROM tenon_jobs.migrations',
  );
  assert.equal(result.rows[0]?.count, '1');
});

test('pgclear scope insertion commits or rolls back with application data', async () => {
  const def = definition();
  let rolledBack = '';
  await assert.rejects(
    pool.transaction(async (tx) => {
      await tx.queryClassic('CREATE TEMP TABLE app_example (id integer) ON COMMIT DROP');
      await tx.queryClassic('INSERT INTO app_example VALUES (1)');
      rolledBack = (await jobs(tx).send(def, 1)).id;
      assert.equal(await jobs(pool).get(rolledBack), null);
      throw new Error('rollback');
    }),
    /rollback/,
  );
  assert.equal(await jobs(pool).get(rolledBack), null);
  const committed = await pool.transaction((tx) => jobs(tx).send(def, 2));
  assert.equal((await jobs(pool).get(committed.id))?.payload, 2);
});

test('Tenon integration joins its transaction and expires with its scope', async () => {
  const db = createTenon({ connection: url });
  const def = definition();
  let rolledBack = '';
  let expired: ReturnType<typeof tenonJobs> | undefined;
  try {
    await assert.rejects(
      db.transaction(async (tx) => {
        await tx.query(unsafeSql('CREATE TEMP TABLE app_example (id integer) ON COMMIT DROP', []));
        await tx.query(unsafeSql('INSERT INTO app_example VALUES (1)', []));
        rolledBack = (await tenonJobs(tx).send(def, 3)).id;
        throw new Error('rollback');
      }),
      /rollback/,
    );
    assert.equal(await jobs(pool).get(rolledBack), null);
    const committed = await db.transaction(async (tx) => {
      expired = tenonJobs(tx);
      return expired.send(def, 4);
    });
    assert.equal((await jobs(pool).get(committed.id))?.payload, 4);
    await assert.rejects(expired!.send(def, 5));
  } finally {
    await db.close();
  }
});

test('key collisions return unchanged outstanding work and terminal records release keys', async () => {
  const def = definition();
  const runAt = new Date(Date.now() + 60_000);
  const initial = await jobs(pool).send(def, 1, { key: 'same', runAt });
  const submissions = await Promise.all(
    Array.from({ length: 20 }, () => jobs(pool).send(def, 2, { key: 'same' })),
  );
  assert.ok(
    submissions.every(
      (job) =>
        job.id === initial.id && job.payload === 1 && job.runAt.getTime() === runAt.getTime(),
    ),
  );
  assert.equal((await jobs(pool).find(def, { key: 'same' })).length, 1);
  await jobs(pool).cancel(initial.id);
  assert.notEqual((await jobs(pool).send(def, 3, { key: 'same' })).id, initial.id);
  assert.notEqual((await jobs(pool).send(def, 4)).id, (await jobs(pool).send(def, 4)).id);
});

test('key contention waits for a committing or rolling-back transaction', async () => {
  const def = definition();
  let pending!: Promise<unknown>;
  let committed = '';
  await pool.transaction(async (tx) => {
    committed = (await jobs(tx).send(def, 1, { key: 'commit' })).id;
    pending = jobs(pool).send(def, 2, { key: 'commit' });
    await delay(20);
  });
  assert.equal(((await pending) as { id: string }).id, committed);
  await assert.rejects(
    pool.transaction(async (tx) => {
      await jobs(tx).send(def, 3, { key: 'rollback' });
      pending = jobs(pool).send(def, 4, { key: 'rollback' });
      await delay(20);
      throw new Error('rollback');
    }),
    /rollback/,
  );
  assert.equal(((await pending) as { payload: number }).payload, 4);
});

test('record edits enforce pending state and report missing and conflicting records', async () => {
  const def = definition();
  const row = await jobs(pool).send(def, 1);
  assert.equal((await jobs(pool).update(def, row.id, 2)).payload, 2);
  const later = new Date(Date.now() + 60000);
  assert.equal((await jobs(pool).reschedule(row.id, later)).runAt.getTime(), later.getTime());
  await jobs(pool).reschedule(row.id, new Date(0));
  const claim = await claimJob(execute, [def.name], 30000);
  assert.ok(claim);
  await assert.rejects(jobs(pool).cancel(row.id), JobStateError);
  await assert.rejects(jobs(pool).reschedule(row.id, later), JobStateError);
  await assert.rejects(jobs(pool).update(def, row.id, 3), JobStateError);
  await assert.rejects(jobs(pool).cancel(randomUUID()), JobNotFoundError);
  assert.equal(await jobs(pool).get(randomUUID()), null);
  await finishClaim(execute, claim, null);
});

test('concurrent claim and cancellation produce exactly one winner', async () => {
  const def = definition();
  const row = await jobs(pool).send(def, 1);
  const [claim, cancellation] = await Promise.allSettled([
    claimJob(execute, [def.name], 30000),
    jobs(pool).cancel(row.id),
  ]);
  assert.equal(claim.status, 'fulfilled');
  if (claim.status === 'fulfilled' && claim.value) {
    assert.equal(cancellation.status, 'rejected');
    if (cancellation.status === 'rejected') assert.ok(cancellation.reason instanceof JobStateError);
    await finishClaim(execute, claim.value, null);
  } else {
    assert.equal(cancellation.status, 'fulfilled');
    assert.equal((await jobs(pool).get(row.id))?.state, 'cancelled');
  }
});

test('multiple runners claim distinct due jobs and leave future or unknown work alone', async () => {
  const handled: number[] = [];
  const def = definition({
    async handler(value) {
      handled.push(value);
      await delay(5);
    },
  });
  const other = definition();
  const work = await Promise.all(
    Array.from({ length: 30 }, (_, index) => jobs(pool).send(def, index)),
  );
  const future = await jobs(pool).send(def, 100, { runAt: new Date(Date.now() + 60000) });
  const unknown = await jobs(pool).send(other, 1);
  const stop = new AbortController();
  const runners = [
    runJobs(pool, [def], { signal: stop.signal, concurrency: 3, pollIntervalMs: 10 }),
    runJobs(pool, [def], { signal: stop.signal, concurrency: 3, pollIntervalMs: 10 }),
  ];
  try {
    await until(
      async () => (await jobs(pool).find(def, { states: ['completed'] })).length === work.length,
    );
  } finally {
    stop.abort();
    await Promise.all(runners);
  }
  assert.equal(handled.length, 30);
  assert.equal(new Set(handled).size, 30);
  assert.equal((await jobs(pool).get(future.id))?.state, 'pending');
  assert.equal((await jobs(pool).get(unknown.id))?.state, 'pending');
  assert.equal((await jobs(pool).attempts(work[0]!.id))[0]?.outcome, 'completed');
});

test('runner retries failures to a fixed total budget and preserves inspection history', async () => {
  const def = definition({
    maxAttempts: 3,
    retryDelayMs: 20,
    handler() {
      throw new Error('mail unavailable');
    },
  });
  const row = await jobs(pool).send(def, 1);
  const stop = new AbortController();
  const running = runJobs(pool, [def], { signal: stop.signal, pollIntervalMs: 5 });
  try {
    await until(async () => (await jobs(pool).get(row.id))?.state === 'failed');
  } finally {
    stop.abort();
    await running;
  }
  const result = await jobs(pool).get(row.id);
  assert.equal(result?.attempts, 3);
  assert.match(result?.lastError ?? '', /mail unavailable/);
  const attempts = await jobs(pool).attempts(row.id);
  assert.deepEqual(
    attempts.map((a) => a.number),
    [3, 2, 1],
  );
  assert.ok(attempts.every((a) => a.outcome === 'failed' && a.finishedAt));
});

test('policy is snapshotted at submission and backoff delays the next claim', async () => {
  const def = definition({ maxAttempts: 2, retryDelayMs: 100 });
  const row = await jobs(pool).send(def, 1);
  const first = await claimJob(execute, [def.name], 30000);
  assert.ok(first);
  await finishClaim(execute, first, 'failure');
  assert.equal(await claimJob(execute, [def.name], 30000), null);
  const stored = await jobs(pool).get(row.id);
  assert.equal(stored?.policy.maxAttempts, 2);
  assert.ok(stored!.runAt.getTime() > Date.now());
  await jobs(pool).reschedule(row.id, new Date(0));
  const second = await claimJob(execute, [def.name], 30000);
  assert.ok(second);
  await finishClaim(execute, second, 'failure');
  assert.equal((await jobs(pool).get(row.id))?.state, 'failed');
});

test('expired claims recover with a fresh token and reject stale outcomes and renewal', async () => {
  const def = definition();
  const row = await jobs(pool).send(def, 1);
  const first = await claimJob(execute, [def.name], 30000);
  assert.ok(first);
  await pool.queryClassic(
    "UPDATE tenon_jobs.jobs SET lease_until = clock_timestamp() - interval '1 second' WHERE id = $1",
    [row.id],
  );
  assert.equal(await finishClaim(execute, first, null), false);
  assert.equal(await renewClaim(execute, first, 30000), false);
  await recoverClaims(execute);
  const second = await claimJob(execute, [def.name], 30000);
  assert.ok(second);
  assert.notEqual(second.claim_token, first.claim_token);
  assert.equal(await finishClaim(execute, first, 'late failure'), false);
  assert.equal(await finishClaim(execute, second, null), true);
  assert.equal((await jobs(pool).get(row.id))?.attempts, 2);
  assert.deepEqual(
    (await jobs(pool).attempts(row.id)).map((a) => a.outcome),
    ['completed', 'expired'],
  );
});

test('heartbeats keep slow work owned without crossing its maximum deadline', async () => {
  let handled = 0;
  const def = definition({
    executionTimeoutMs: 3000,
    async handler() {
      handled++;
      await delay(500);
    },
  });
  const row = await jobs(pool).send(def, 1);
  const stop = new AbortController();
  const running = runJobs(pool, [def], {
    signal: stop.signal,
    leaseMs: 150,
    pollIntervalMs: 5,
    concurrency: 2,
  });
  try {
    await until(async () => (await jobs(pool).get(row.id))?.state === 'completed');
  } finally {
    stop.abort();
    await running;
  }
  assert.equal(handled, 1);
  assert.equal((await jobs(pool).get(row.id))?.attempts, 1);
});

test('execution deadlines bound uncooperative handlers and exhaust their attempts', async () => {
  const def = definition({
    maxAttempts: 2,
    executionTimeoutMs: 80,
    handler() {
      return new Promise(() => {});
    },
  });
  const row = await jobs(pool).send(def, 1);
  const stop = new AbortController();
  const running = runJobs(pool, [def], { signal: stop.signal, leaseMs: 90, pollIntervalMs: 5 });
  try {
    await until(async () => (await jobs(pool).get(row.id))?.state === 'failed');
  } finally {
    stop.abort();
    await running;
  }
  assert.equal((await jobs(pool).get(row.id))?.attempts, 2);
});

test('shutdown allows cooperative completion and keeps the borrowed pool open', async () => {
  let entered = false;
  const def = definition({
    async handler() {
      entered = true;
      await delay(100);
    },
  });
  const row = await jobs(pool).send(def, 1);
  const stop = new AbortController();
  const running = runJobs(pool, [def], {
    signal: stop.signal,
    shutdownTimeoutMs: 1000,
    pollIntervalMs: 5,
  });
  await until(async () => entered);
  stop.abort();
  await running;
  assert.equal((await jobs(pool).get(row.id))?.state, 'completed');
  assert.equal((await pool.queryClassic<{ value: number }>('SELECT 1 AS value')).rows[0]?.value, 1);
});

test('shutdown expiry signals handlers and abandons fenced claims for recovery', async () => {
  let signal: AbortSignal | undefined;
  const def = definition({
    handler(_payload, context) {
      signal = context.signal;
      return new Promise(() => {});
    },
  });
  const row = await jobs(pool).send(def, 1);
  const stop = new AbortController();
  const running = runJobs(pool, [def], {
    signal: stop.signal,
    shutdownTimeoutMs: 20,
    leaseMs: 150,
    pollIntervalMs: 5,
  });
  await until(async () => signal !== undefined);
  stop.abort();
  await running;
  assert.equal(signal?.aborted, true);
  assert.equal((await jobs(pool).get(row.id))?.state, 'running');
  await delay(160);
  await recoverClaims(execute);
  assert.equal((await jobs(pool).get(row.id))?.state, 'pending');
});

test('a killed worker process leaves a recoverable claim', async () => {
  const def = definition();
  const row = await jobs(pool).send(def, 1);
  const child = fork(new URL('../fixtures/worker.js', import.meta.url), [def.name], {
    env: { ...process.env, TENON_JOBS_TEST_DATABASE_URL: url },
    stdio: ['ignore', 'ignore', 'inherit', 'ipc'],
  });
  const exited = once(child, 'exit');
  try {
    const message = await Promise.race([
      once(child, 'message'),
      exited.then(() => {
        throw new Error('Worker exited before claiming.');
      }),
      delay(5000).then(() => {
        throw new Error('Worker did not claim.');
      }),
    ]);
    assert.equal(message[0], row.id);
    child.kill('SIGKILL');
    await exited;
    await delay(600);
    await recoverClaims(execute);
    assert.equal((await jobs(pool).get(row.id))?.state, 'pending');
    const next = await claimJob(execute, [def.name], 30000);
    assert.ok(next);
    await finishClaim(execute, next, null);
    assert.equal((await jobs(pool).get(row.id))?.attempts, 2);
  } finally {
    if (child.exitCode === null && child.signalCode === null) {
      child.kill('SIGKILL');
      await exited;
    }
  }
});

test('driver failures reject the runner without closing the application pool', async () => {
  const limited = createPool({ connectionString: url, maxConnections: 1 });
  const lease = await limited.acquire();
  try {
    await assert.rejects(runJobs(limited, [definition()], { databaseTimeoutMs: 50 }));
  } finally {
    await lease.release();
  }
  try {
    assert.equal(
      (await limited.queryClassic<{ value: number }>('SELECT 1 AS value')).rows[0]?.value,
      1,
    );
  } finally {
    await limited.close();
  }
});
