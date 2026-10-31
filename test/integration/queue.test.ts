import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { fork } from 'node:child_process';
import { setTimeout as delay } from 'node:timers/promises';
import { once } from 'node:events';
import test from 'node:test';
import { createPool, listen } from '@kequtech/pgclear';
import { createTenon, unsafeSql } from '@kequtech/tenon-orm';
import { installJobs, jobs, JobNotFoundError, JobStateError, runJobs } from '../../src/index.js';
import { jobs as tenonJobs } from '../../src/tenon.js';
import { claimJob as fetchJob, finishClaim, recoverClaims, renewClaim } from '../../src/claims.js';
import type { Execute } from '../../src/storage.js';
import { notificationChannel } from '../../src/schema.js';

import { definition, execute, pool, until, url } from '../helpers/database.js';

async function claimJob(execute: Execute, names: readonly string[], leaseMs: number) {
  return (await fetchJob(execute, names, leaseMs)).claim;
}
test('installation is repeatable and safe across concurrent installers', async () => {
  await Promise.all([installJobs(pool), installJobs(pool), installJobs(pool)]);
  const result = await pool.queryClassic<{ count: string }>(
    'SELECT count(*)::text AS count FROM tenon_jobs.migrations',
  );
  assert.equal(result.rows[0]?.count, '3');
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

test('heartbeats extend a live claim while its handler remains active', async () => {
  const release = Promise.withResolvers<void>();
  let handled = 0;
  const def = definition({
    executionTimeoutMs: 10000,
    async handler() {
      handled++;
      await release.promise;
    },
  });
  const row = await jobs(pool).send(def, 1);
  const stop = new AbortController();
  const running = runJobs(pool, [def], {
    signal: stop.signal,
    leaseMs: 3000,
    pollIntervalMs: 10,
    concurrency: 2,
    shutdownTimeoutMs: 20,
  });
  async function leaseTime(): Promise<number> {
    const result = await pool.queryClassic<{ lease: string | null }>(
      'SELECT to_jsonb(lease_until) AS lease FROM tenon_jobs.jobs WHERE id = $1',
      [row.id],
    );
    return result.rows[0]?.lease ? new Date(result.rows[0].lease).getTime() : 0;
  }
  try {
    await until(async () => handled === 1);
    const initial = await leaseTime();
    assert.ok(initial > 0);
    await until(async () => (await leaseTime()) >= initial + 1500);
    assert.equal((await jobs(pool).get(row.id))?.state, 'running');
    release.resolve();
    await until(async () => (await jobs(pool).get(row.id))?.state === 'completed');
  } finally {
    stop.abort();
    release.resolve();
    await running;
  }
  assert.equal(handled, 1);
  assert.equal((await jobs(pool).get(row.id))?.attempts, 1);
});

test('execution deadlines exhaust attempts after cooperative cancellation', async () => {
  const def = definition({
    maxAttempts: 2,
    executionTimeoutMs: 80,
    async handler(_payload, { signal }) {
      await delay(10_000, undefined, { signal });
    },
  });
  const row = await jobs(pool).send(def, 1);
  const stop = new AbortController();
  const running = runJobs(pool, [def], {
    signal: stop.signal,
    leaseMs: 90,
    pollIntervalMs: 5,
    recoveryIntervalMs: 10,
  });
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

async function listenerPid(applicationName: string): Promise<number | undefined> {
  const { rows } = await pool.queryClassic<{ pid: number }>(
    `SELECT pid FROM pg_stat_activity WHERE application_name = $1
     AND state = 'idle' AND query LIKE 'LISTEN %'`,
    [applicationName],
  );
  return rows[0]?.pid;
}

const quietRunner = { pollIntervalMs: 30_000, recoveryIntervalMs: 30_000, shutdownTimeoutMs: 20 };

test('upgrading version 1 preserves jobs and installs transactional notifications', async () => {
  const def = definition();
  const rollback = new Error('rollback upgrade fixture');
  await assert.rejects(
    pool.transaction(async (tx) => {
      const row = await jobs(tx).send(def, 7);
      await tx.queryClassic('DROP TABLE tenon_jobs.schedules');
      await tx.queryClassic(
        'ALTER TABLE tenon_jobs.jobs DROP COLUMN schedule_id, DROP COLUMN scheduled_for',
      );
      await tx.queryClassic('DROP TRIGGER notify_pending ON tenon_jobs.jobs');
      await tx.queryClassic('DROP FUNCTION tenon_jobs.notify_pending()');
      await tx.queryClassic('DELETE FROM tenon_jobs.migrations WHERE version >= 2');
      await installJobs({ transaction: (work) => tx.savepoint(work) });
      assert.equal((await jobs(tx).get(row.id))?.payload, 7);
      const version = await tx.queryClassic<{ version: number }>(
        'SELECT max(version) AS version FROM tenon_jobs.migrations',
      );
      assert.equal(version.rows[0]?.version, 3);
      const trigger = await tx.queryClassic<{ enabled: string }>(
        "SELECT tgenabled AS enabled FROM pg_trigger WHERE tgrelid = 'tenon_jobs.jobs'::regclass AND tgname = 'notify_pending'",
      );
      assert.equal(trigger.rows[0]?.enabled, 'O');
      throw rollback;
    }),
    (error) => error === rollback,
  );
});

test('notifications commit with pgclear and Tenon operations and ignore keyed no-op updates', async () => {
  const def = definition();
  const received: string[] = [];
  const listener = await listen(url!, {
    channels: [notificationChannel],
    onNotification: ({ payload }) => {
      if (payload === def.name) received.push(payload);
    },
  });
  const db = createTenon({ connection: url });
  try {
    await assert.rejects(
      pool.transaction(async (tx) => {
        await jobs(tx).send(def, 1);
        await delay(30);
        assert.equal(received.length, 0);
        throw new Error('rollback');
      }),
      /rollback/,
    );
    await db.transaction(async (tx) => {
      await tenonJobs(tx).send(def, 2, { key: 'commit' });
      await delay(30);
      assert.equal(received.length, 0);
    });
    await until(async () => received.length === 1);
    await jobs(pool).send(def, 3, { key: 'commit' });
    await delay(30);
    assert.equal(received.length, 1);
    const row = (await jobs(pool).find(def))[0]!;
    await jobs(pool).reschedule(row.id, new Date(Date.now() + 60_000));
    await until(async () => received.length === 2);
  } finally {
    await listener.close();
    await db.close();
  }
});

for (const loss of ['timeout', 'lease loss'] as const) {
  test(`${loss} holds the local slot while independent recovery continues`, async () => {
    const release = Promise.withResolvers<void>();
    let active = 0;
    let maximum = 0;
    let started = 0;
    let handlerSignal: AbortSignal | undefined;
    const def = definition({
      maxAttempts: 1,
      executionTimeoutMs: loss === 'timeout' ? 100 : 5000,
      async handler(value, { signal }) {
        started++;
        active++;
        maximum = Math.max(maximum, active);
        try {
          if (value === 1) {
            handlerSignal = signal;
            await release.promise;
          }
        } finally {
          active--;
        }
      },
    });
    const first = await jobs(pool).send(def, 1);
    const second = await jobs(pool).send(def, 2);
    const stop = new AbortController();
    const running = runJobs(pool, [def], {
      signal: stop.signal,
      concurrency: 1,
      leaseMs: 90,
      recoveryIntervalMs: 10,
      pollIntervalMs: 10,
      shutdownTimeoutMs: 20,
    });
    try {
      await until(async () => started === 1);
      if (loss === 'lease loss') {
        await pool.queryClassic(
          "UPDATE tenon_jobs.jobs SET lease_until = clock_timestamp() - interval '1 second' WHERE id = $1",
          [first.id],
        );
      }
      await until(async () => (await jobs(pool).get(first.id))?.state === 'failed');
      await until(async () => handlerSignal?.aborted === true);
      await delay(80);
      assert.equal(started, 1);
      assert.equal(active, 1);
      assert.equal((await jobs(pool).get(second.id))?.state, 'pending');
      release.resolve();
      await until(async () => (await jobs(pool).get(second.id))?.state === 'completed');
      assert.equal(maximum, 1);
      assert.equal((await jobs(pool).get(first.id))?.state, 'failed');
    } finally {
      stop.abort();
      release.resolve();
      await running;
    }
  });
}

test('a borrowed pool runner wakes on committed work with a long polling interval', async () => {
  let handled = 0;
  const def = definition({
    handler() {
      handled++;
    },
  });
  const stop = new AbortController();
  const running = runJobs(pool, [def], {
    ...quietRunner,
    signal: stop.signal,
    notifications: { connectionString: url!, applicationName: def.name },
  });
  try {
    await until(async () => Boolean(await listenerPid(def.name)));
    await pool.transaction(async (tx) => {
      await jobs(tx).send(def, 1);
      await delay(50);
      assert.equal(handled, 0);
    });
    await until(async () => handled === 1, 2000);
  } finally {
    stop.abort();
    await running;
  }
  assert.equal(await listenerPid(def.name), undefined);
  await pool.queryClassic('SELECT 1');
});

test('connection settings create a runner-owned pool and listener and close both', async () => {
  let handled = 0;
  const def = definition({
    handler() {
      handled++;
    },
  });
  const stop = new AbortController();
  const running = runJobs(
    { connectionString: url!, applicationName: def.name, maxConnections: 2 },
    [def],
    {
      ...quietRunner,
      signal: stop.signal,
    },
  );
  try {
    await until(async () => Boolean(await listenerPid(def.name)));
    await jobs(pool).send(def, 1);
    await until(async () => handled === 1, 2000);
  } finally {
    stop.abort();
    await running;
  }
  const result = await pool.queryClassic<{ count: string }>(
    'SELECT count(*)::text AS count FROM pg_stat_activity WHERE application_name = $1',
    [def.name],
  );
  assert.equal(result.rows[0]?.count, '0');
});

test('future work, retries and earlier reschedules wake without waiting for fallback polling', async () => {
  const called: { value: number; at: number }[] = [];
  const def = definition({
    retryDelayMs: 150,
    retryBackoff: 'fixed',
    handler(value, { attempt }) {
      called.push({ value, at: Date.now() });
      if (value === 1 && attempt === 1) throw new Error('retry once');
    },
  });
  const stop = new AbortController();
  const running = runJobs(pool, [def], {
    ...quietRunner,
    signal: stop.signal,
    notifications: { connectionString: url!, applicationName: def.name },
  });
  try {
    await until(async () => Boolean(await listenerPid(def.name)));
    const runAt = new Date(Date.now() + 150);
    const delayed = await jobs(pool).send(def, 1, { runAt });
    await until(async () => (await jobs(pool).get(delayed.id))?.state === 'completed', 2000);
    assert.equal(called.length, 2);
    assert.ok(called[0]!.at >= runAt.getTime());
    assert.ok(called[1]!.at - called[0]!.at >= 150);
    const later = await jobs(pool).send(def, 2, { runAt: new Date(Date.now() + 60_000) });
    await delay(30);
    await jobs(pool).reschedule(later.id, new Date(0));
    await until(async () => (await jobs(pool).get(later.id))?.state === 'completed', 2000);
  } finally {
    stop.abort();
    await running;
  }
});

test('fallback polling discovers work after an unnotified row-lock release', async () => {
  const def = definition();
  const row = await jobs(pool).send(def, 1);
  const stop = new AbortController();
  let running: Promise<void> | undefined;
  try {
    await pool.transaction(async (tx) => {
      await tx.queryClassic('SELECT id FROM tenon_jobs.jobs WHERE id = $1 FOR UPDATE', [row.id]);
      running = runJobs(pool, [def], {
        ...quietRunner,
        signal: stop.signal,
        pollIntervalMs: 80,
        notifications: { connectionString: url!, applicationName: def.name },
      });
      await until(async () => Boolean(await listenerPid(def.name)));
      await delay(100);
      assert.equal((await jobs(tx).get(row.id))?.state, 'pending');
    });
    await until(async () => (await jobs(pool).get(row.id))?.state === 'completed', 2000);
  } finally {
    stop.abort();
    await running;
  }
});

test('listener reconnection reconciles work and restores notifications', async (t) => {
  t.mock.method(process, 'emitWarning', () => {});
  let handled = 0;
  const def = definition({
    handler() {
      handled++;
    },
  });
  const stop = new AbortController();
  const running = runJobs(pool, [def], {
    ...quietRunner,
    signal: stop.signal,
    notifications: { connectionString: url!, applicationName: def.name },
  });
  try {
    await until(async () => Boolean(await listenerPid(def.name)));
    const original = (await listenerPid(def.name))!;
    await pool.queryClassic('SELECT pg_terminate_backend($1)', [original]);
    await jobs(pool).send(def, 1);
    await until(async () => handled === 1, 2000);
    await until(async () => {
      const next = await listenerPid(def.name);
      return next !== undefined && next !== original;
    });
    await jobs(pool).send(def, 2);
    await until(async () => handled === 2, 2000);
  } finally {
    stop.abort();
    await running;
  }
});

test('an initial listener failure polls, retries registration and recovers', async (t) => {
  const warning = t.mock.method(process, 'emitWarning', () => {});
  let passwordCalls = 0;
  let handled = 0;
  const def = definition({
    handler() {
      handled++;
    },
  });
  const stop = new AbortController();
  const parsed = new URL(url!);
  const running = runJobs(pool, [def], {
    ...quietRunner,
    signal: stop.signal,
    pollIntervalMs: 80,
    notifications: {
      connectionString: url!,
      applicationName: def.name,
      password() {
        if (passwordCalls++ === 0) throw new Error('temporary credential provider failure');
        return decodeURIComponent(parsed.password);
      },
    },
  });
  try {
    await until(async () => warning.mock.callCount() > 0);
    await jobs(pool).send(def, 1);
    await until(async () => handled === 1, 2000);
    await until(async () => Boolean(await listenerPid(def.name)));
    assert.ok(passwordCalls >= 2);
  } finally {
    stop.abort();
    await running;
  }
});

test('shutdown bounds the wait for an already timed-out handler', async () => {
  const release = Promise.withResolvers<void>();
  const def = definition({
    maxAttempts: 1,
    executionTimeoutMs: 60,
    async handler() {
      await release.promise;
    },
  });
  const row = await jobs(pool).send(def, 1);
  const stop = new AbortController();
  const running = runJobs(pool, [def], {
    signal: stop.signal,
    leaseMs: 90,
    recoveryIntervalMs: 10,
    shutdownTimeoutMs: 20,
  });
  try {
    await until(async () => (await jobs(pool).get(row.id))?.state === 'failed');
    stop.abort();
    await Promise.race([
      running,
      delay(1000).then(() => {
        throw new Error('Shutdown waited for an uncooperative handler');
      }),
    ]);
  } finally {
    stop.abort();
    release.resolve();
    await running;
  }
});

test('a database failure during active work stops the runner and signals its handler', async () => {
  const driver = createPool(url!);
  const release = Promise.withResolvers<void>();
  let signal: AbortSignal | undefined;
  const def = definition({
    async handler(_payload, context) {
      signal = context.signal;
      await release.promise;
    },
  });
  await jobs(pool).send(def, 1);
  const running = runJobs(driver, [def], { recoveryIntervalMs: 20 });
  const outcome = running.then(
    () => null,
    (error: unknown) => error,
  );
  try {
    await until(async () => signal !== undefined);
    await driver.close({ timeoutMs: 100 });
    assert.ok((await outcome) instanceof Error);
    assert.equal(signal?.aborted, true);
  } finally {
    release.resolve();
    await driver.close();
  }
});

test('configured concurrency above ten holds slots without internal listener warnings', async (t) => {
  const warning = t.mock.method(process, 'emitWarning', () => {});
  const release = Promise.withResolvers<void>();
  let active = 0;
  const def = definition({
    async handler() {
      active++;
      try {
        await release.promise;
      } finally {
        active--;
      }
    },
  });
  const work = await Promise.all(Array.from({ length: 16 }, () => jobs(pool).send(def, 1)));
  const stop = new AbortController();
  const running = runJobs(pool, [def], {
    signal: stop.signal,
    concurrency: 16,
    shutdownTimeoutMs: 20,
  });
  try {
    await until(async () => active === 16);
    assert.equal(warning.mock.callCount(), 0);
    release.resolve();
    await until(
      async () => (await jobs(pool).find(def, { states: ['completed'] })).length === work.length,
    );
  } finally {
    stop.abort();
    release.resolve();
    await running;
  }
});
