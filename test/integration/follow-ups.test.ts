import assert from 'node:assert/strict';
import { fork } from 'node:child_process';
import { once } from 'node:events';
import { setTimeout as delay } from 'node:timers/promises';
import test from 'node:test';
import { listen } from '@kequtech/pgclear';
import { defineJob, installJobs, jobs, JobError, JobStateError, runJobs } from '../../src/index.js';
import { claimJob, finishClaim, recoverClaims } from '../../src/claims.js';
import { checkSchema, notificationChannel } from '../../src/schema.js';
import { definition, execute, pool, until, url } from '../helpers/database.js';

async function claim(name: string) {
  return (await claimJob(execute, [name], 30_000)).claim!;
}

test('requests coalesce unstarted work and preserve existing timing and execution policy', async () => {
  const def = definition({ maxAttempts: 2 });
  const concurrent = await Promise.all(
    Array.from({ length: 8 }, (_, payload) => jobs(pool).request(def, payload, { key: 'one' })),
  );
  assert.equal(new Set(concurrent.map((job) => job.id)).size, 1);
  const original = concurrent[0]!;
  const changed = defineJob({ ...def, maxAttempts: 5 });
  const delayed = await jobs(pool).request(changed, 10, { key: 'one', delayMs: 60_000 });
  assert.equal(delayed.id, original.id);
  assert.equal(delayed.policy.maxAttempts, 2);
  assert.equal(delayed.attempts, 0);
  assert.equal(delayed.blockedBy, null);
  assert.ok(delayed.runAt.getTime() >= Date.now() + 59_000);
  const latest = await jobs(pool).request(def, 11, { key: 'one' });
  assert.equal(latest.payload, 11);
  assert.equal(latest.runAt.getTime(), delayed.runAt.getTime());
  const otherKey = await jobs(pool).request(def, 12, { key: 'two' });
  const otherDefinition = await jobs(pool).request(definition(), 13, { key: 'one' });
  assert.notEqual(otherKey.id, original.id);
  assert.notEqual(otherDefinition.id, original.id);
});

test('running requests retain one inspectable successor without changing the current job', async () => {
  const def = definition({ maxAttempts: 2 });
  const original = await jobs(pool).request(def, 1, { key: 'one' });
  const first = await claim(def.name);
  const current = await jobs(pool).get(original.id);
  const changed = defineJob({ ...def, maxAttempts: 4 });
  const submitted = await Promise.all(
    Array.from({ length: 8 }, (_, payload) => jobs(pool).request(changed, payload, { key: 'one' })),
  );
  assert.equal(new Set(submitted.map((job) => job.id)).size, 1);
  const successor = await jobs(pool).request(def, 99, { key: 'one', delayMs: 60_000 });
  assert.equal(successor.id, submitted[0]!.id);
  assert.notEqual(successor.id, original.id);
  assert.equal(successor.blockedBy, original.id);
  assert.equal(successor.policy.maxAttempts, 4);
  assert.equal(successor.attempts, 0);
  assert.deepEqual(await jobs(pool).get(original.id), current);
  assert.deepEqual(await jobs(pool).attempts(successor.id), []);
  assert.equal((await jobs(pool).find(def, { key: 'one' })).length, 2);
  assert.deepEqual(await claimJob(execute, [def.name], 30_000), { claim: null, delayMs: null });
  const stats = await jobs(pool).stats(def);
  assert.equal(stats.pending, 1);
  assert.equal(stats.blocked, 1);
  assert.equal(stats.ready, 0);
  assert.equal(stats.delayed, 0);
  const edited = await jobs(pool).update(def, successor.id, 100);
  assert.equal(edited.blockedBy, original.id);
  await jobs(pool).reschedule(successor.id, new Date(0));
  assert.equal(await finishClaim(execute, first, null), true);
  const released = (await jobs(pool).get(successor.id))!;
  assert.equal(released.blockedBy, null);
  assert.equal(released.payload, 100);
  assert.equal((await jobs(pool).stats(def)).ready, 1);
  assert.equal((await jobs(pool).stats(def)).blocked, 0);
  const next = await claim(def.name);
  assert.equal(next.id, successor.id);
  assert.equal(next.attempt_count, 1);
  await finishClaim(execute, next, null);
});

test('a first follow-up requested during retry backoff preserves the original payload and retry time', async () => {
  const def = definition({ maxAttempts: 3, retryDelayMs: 60_000 });
  const original = await jobs(pool).request(def, 1, { key: 'one' });
  await finishClaim(execute, await claim(def.name), 'temporary');
  const retry = (await jobs(pool).get(original.id))!;
  assert.equal(retry.state, 'pending');
  const successor = await jobs(pool).request(def, 2, { key: 'one' });
  const latest = await jobs(pool).request(def, 3, { key: 'one' });
  assert.equal(latest.id, successor.id);
  assert.equal(latest.blockedBy, original.id);
  assert.equal(latest.payload, 3);
  assert.deepEqual(await jobs(pool).get(original.id), retry);
  const waiting = await claimJob(execute, [def.name], 30_000);
  assert.equal(waiting.claim, null);
  assert.ok(waiting.delayMs! > 59_000);
  await jobs(pool).reschedule(original.id, new Date(0));
  const next = await claim(def.name);
  assert.equal(next.id, original.id);
  assert.equal(next.payload, 1);
  assert.equal(next.attempt_count, 2);
  await finishClaim(execute, next, null);
  assert.equal((await claim(def.name)).id, successor.id);
});

for (const ending of [
  'completed',
  'permanent failure',
  'exhausted',
  'expired',
  'cancelled',
] as const) {
  test(`${ending} releases a successor atomically with a fresh budget`, async () => {
    const def = definition({ maxAttempts: ending === 'cancelled' ? 2 : 1 });
    const original = await jobs(pool).request(def, 1, { key: 'one' });
    const current = await claim(def.name);
    const successor = await jobs(pool).request(def, 2, { key: 'one' });
    if (ending === 'expired') {
      await pool.queryClassic(
        "UPDATE pgclear_jobs.jobs SET lease_until = clock_timestamp() - interval '1 second' WHERE id = $1",
        [original.id],
      );
      await recoverClaims(execute);
      assert.equal(await finishClaim(execute, current, null), false);
    } else if (ending === 'cancelled') {
      // Use a real retry transition before cancellation rather than manufacturing a pending row.
      await assert.rejects(jobs(pool).cancel(original.id), JobStateError);
      await finishClaim(execute, current, 'temporary');
      await jobs(pool).cancel(original.id);
    } else {
      await finishClaim(
        execute,
        current,
        ending === 'completed'
          ? null
          : ending === 'permanent failure'
            ? new JobError('stop', { retry: false })
            : 'exhausted',
      );
    }
    assert.equal((await jobs(pool).get(successor.id))?.blockedBy, null);
    const next = await claim(def.name);
    assert.equal(next.id, successor.id);
    assert.equal(next.attempt_count, 1);
    assert.equal(next.payload, 2);
    await finishClaim(execute, next, null);
    assert.equal((await jobs(pool).get(successor.id))?.state, 'completed');
  });
}

test('expired claims keep follow-ups blocked through automatic recovery retries', async () => {
  const def = definition({ maxAttempts: 2, retryDelayMs: 60_000 });
  const original = await jobs(pool).request(def, 1, { key: 'one' });
  const stale = await claim(def.name);
  const successor = await jobs(pool).request(def, 2, { key: 'one' });
  await pool.queryClassic(
    "UPDATE pgclear_jobs.jobs SET lease_until = clock_timestamp() - interval '1 second' WHERE id = $1",
    [original.id],
  );
  await recoverClaims(execute);
  const retry = (await jobs(pool).get(original.id))!;
  assert.equal(retry.state, 'pending');
  assert.equal(retry.payload, 1);
  assert.equal((await jobs(pool).get(successor.id))?.blockedBy, original.id);
  assert.equal(await finishClaim(execute, stale, null), false);
  await jobs(pool).cancel(original.id);
  assert.equal((await claim(def.name)).id, successor.id);
});

test('a successor keeps its timing and can itself acquire a fresh follow-up after starting', async () => {
  const def = definition();
  await jobs(pool).request(def, 1, { key: 'one' });
  const original = await claim(def.name);
  const successor = await jobs(pool).request(def, 2, { key: 'one', delayMs: 60_000 });
  await finishClaim(execute, original, null);
  assert.equal((await jobs(pool).get(successor.id))?.runAt.getTime(), successor.runAt.getTime());
  assert.equal((await claimJob(execute, [def.name], 30_000)).claim, null);
  const updated = await jobs(pool).request(def, 3, { key: 'one', runAt: new Date(0) });
  assert.equal(updated.id, successor.id);
  const second = await claim(def.name);
  const third = await jobs(pool).request(def, 4, { key: 'one' });
  assert.equal(third.blockedBy, successor.id);
  await finishClaim(execute, second, null);
  assert.equal((await claim(def.name)).id, third.id);
});

test('cancelling a blocked successor permits a replacement and terminal cleanup preserves it', async () => {
  const def = definition();
  const original = await jobs(pool).request(def, 1, { key: 'one' });
  const current = await claim(def.name);
  const cancelled = await jobs(pool).request(def, 2, { key: 'one' });
  assert.equal((await jobs(pool).cancel(cancelled.id)).blockedBy, null);
  const replacement = await jobs(pool).request(def, 3, { key: 'one' });
  assert.notEqual(replacement.id, cancelled.id);
  await assert.rejects(jobs(pool).remove(replacement.id), JobStateError);
  await finishClaim(execute, current, null);
  assert.equal(await jobs(pool).prune(def, { before: new Date(Date.now() + 1000) }), 2);
  assert.equal(await jobs(pool).get(original.id), null);
  assert.equal((await claim(def.name)).id, replacement.id);
});

test('manual retry refuses to displace a released successor and leaves the transaction usable', async () => {
  const def = definition({ maxAttempts: 1 });
  const original = await jobs(pool).request(def, 1, { key: 'one' });
  const current = await claim(def.name);
  const successor = await jobs(pool).request(def, 2, { key: 'one' });
  await finishClaim(execute, current, 'failed');
  await pool.transaction(async (tx) => {
    await assert.rejects(jobs(tx).retry(original.id), JobStateError);
    assert.equal((await jobs(tx).request(def, 3, { key: 'one' })).id, successor.id);
    await tx.queryClassic('SELECT 1');
  });
  assert.equal((await jobs(pool).get(successor.id))?.payload, 3);
});

test('requests and promotions join the caller transaction', async () => {
  const def = definition();
  const original = await jobs(pool).request(def, 1, { key: 'one' });
  const current = await claim(def.name);
  let successorId = '';
  const rollback = new Error('rollback requests');
  await assert.rejects(
    pool.transaction(async (tx) => {
      successorId = (await jobs(tx).request(def, 2, { key: 'one' })).id;
      assert.equal(await jobs(pool).get(successorId), null);
      throw rollback;
    }),
    (error) => error === rollback,
  );
  assert.equal(await jobs(pool).get(successorId), null);
  const successor = await pool.transaction((tx) => jobs(tx).request(def, 3, { key: 'one' }));
  await assert.rejects(
    pool.transaction(async (tx) => {
      await jobs(tx).request(def, 4, { key: 'one' });
      await finishClaim((text, values) => tx.queryClassic(text, values), current, null);
      assert.equal((await jobs(tx).get(successor.id))?.blockedBy, null);
      throw rollback;
    }),
    (error) => error === rollback,
  );
  assert.equal((await jobs(pool).get(successor.id))?.payload, 3);
  assert.equal((await jobs(pool).get(successor.id))?.blockedBy, original.id);
  assert.equal((await jobs(pool).get(original.id))?.state, 'running');
  await finishClaim(execute, current, null);
  assert.equal((await claim(def.name)).id, successor.id);
});

test('requests racing claims and completion leave the latest work durably executable', async () => {
  for (let index = 0; index < 12; index++) {
    const def = definition();
    const original = await jobs(pool).request(def, 1, { key: 'one' });
    const [requested, competingClaim] = await Promise.all([
      jobs(pool).request(def, 2, { key: 'one' }),
      claim(def.name),
    ]);
    // SKIP LOCKED may deliberately pass over the row while request updates it.
    const claimed = competingClaim ?? (await claim(def.name));
    assert.ok(claimed);
    if (requested.id === original.id) {
      assert.equal(claimed.payload, 2);
      await finishClaim(execute, claimed, null);
    } else {
      assert.equal(claimed.payload, 1);
      assert.equal(requested.blockedBy, original.id);
      await finishClaim(execute, claimed, null);
      await finishClaim(execute, await claim(def.name), null);
    }
    await jobs(pool).request(def, 3, { key: 'one' });
    const active = await claim(def.name);
    const [latest] = await Promise.all([
      jobs(pool).request(def, 4, { key: 'one' }),
      finishClaim(execute, active, null),
    ]);
    assert.equal(latest.payload, 4);
    const next = await claim(def.name);
    assert.equal(next.id, latest.id);
    assert.equal(next.payload, 4);
    await finishClaim(execute, next, null);
    assert.equal((await jobs(pool).stats(def)).pending, 0);
  }
});

test('two runners serialize a key through retries while other keys continue', async () => {
  const release = Promise.withResolvers<void>();
  const active = new Set<string>();
  const calls: number[] = [];
  let overlap = false;
  const def = definition({
    maxAttempts: 2,
    retryDelayMs: 80,
    async handler(value, { attempt }) {
      const key = value < 10 ? 'one' : 'two';
      if (active.has(key)) overlap = true;
      active.add(key);
      calls.push(value);
      try {
        if (value === 1 && attempt === 1) {
          await release.promise;
          throw new Error('retry once');
        }
        await delay(20);
      } finally {
        active.delete(key);
      }
    },
  });
  const original = await jobs(pool).request(def, 1, { key: 'one' });
  const stop = new AbortController();
  const options = {
    signal: stop.signal,
    concurrency: 3,
    pollIntervalMs: 5,
    shutdownTimeoutMs: 1000,
  };
  const runners = [runJobs(pool, [def], options), runJobs(pool, [def], options)];
  try {
    await until(async () => calls.includes(1));
    const successor = await jobs(pool).request(def, 2, { key: 'one' });
    const other = await jobs(pool).request(def, 10, { key: 'two' });
    await until(async () => (await jobs(pool).get(other.id))?.state === 'completed');
    assert.equal((await jobs(pool).get(successor.id))?.blockedBy, original.id);
    assert.deepEqual(
      calls.filter((value) => value < 10),
      [1],
    );
    release.resolve();
    await until(async () => (await jobs(pool).get(successor.id))?.state === 'completed');
    assert.deepEqual(
      calls.filter((value) => value < 10),
      [1, 1, 2],
    );
    assert.equal(overlap, false);
  } finally {
    stop.abort();
    release.resolve();
    await Promise.all(runners);
  }
});

test('a killed worker retains its successor through recovery and retry', async () => {
  const def = definition({ maxAttempts: 2 });
  const original = await jobs(pool).request(def, 1, { key: 'one' });
  const child = fork(new URL('../fixtures/worker.js', import.meta.url), [def.name], {
    env: { ...process.env, PGCLEAR_JOBS_TEST_DATABASE_URL: url },
    stdio: ['ignore', 'ignore', 'inherit', 'ipc'],
  });
  const exited = once(child, 'exit');
  try {
    const [message] = await Promise.race([
      once(child, 'message'),
      exited.then(() => {
        throw new Error('Worker exited before claiming.');
      }),
      delay(5000).then(() => {
        throw new Error('Worker did not claim.');
      }),
    ]);
    assert.equal(message, original.id);
    const successor = await jobs(pool).request(def, 2, { key: 'one' });
    child.kill('SIGKILL');
    await exited;
    await delay(600);
    await recoverClaims(execute);
    assert.equal((await jobs(pool).get(successor.id))?.blockedBy, original.id);
    const retry = await claim(def.name);
    assert.equal(retry.id, original.id);
    assert.equal(retry.payload, 1);
    await finishClaim(execute, retry, null);
    assert.equal((await claim(def.name)).id, successor.id);
  } finally {
    if (child.exitCode === null && child.signalCode === null) {
      child.kill('SIGKILL');
      await exited;
    }
  }
});

test('successor promotion notifies only after commit and wakes a runner without fallback polling', async () => {
  const def = definition();
  await jobs(pool).request(def, 1, { key: 'one' });
  const current = await claim(def.name);
  let notifications = 0;
  const listener = await listen(url, {
    channels: [notificationChannel],
    onNotification: ({ payload }) => {
      if (payload === def.name) notifications++;
    },
  });
  const stop = new AbortController();
  let runner: Promise<void> | undefined;
  try {
    const successor = await jobs(pool).request(def, 2, { key: 'one' });
    await delay(30);
    assert.equal(notifications, 0);
    runner = runJobs(pool, [def], {
      signal: stop.signal,
      pollIntervalMs: 30_000,
      recoveryIntervalMs: 30_000,
      notifications: url,
    });
    await delay(100);
    assert.equal((await jobs(pool).get(successor.id))?.blockedBy, current.id);
    await pool.transaction(async (tx) => {
      await finishClaim((text, values) => tx.queryClassic(text, values), current, null);
      await delay(30);
      assert.equal(notifications, 0);
      assert.equal((await jobs(pool).get(successor.id))?.blockedBy, current.id);
    });
    await until(async () => (await jobs(pool).get(successor.id))?.state === 'completed', 2000);
    assert.equal(notifications, 1);
  } finally {
    stop.abort();
    await runner;
    await listener.close();
  }
});

test('invalid requests fail before SQL and leave the transaction usable', async () => {
  const def = definition();
  await pool.transaction(async (tx) => {
    await assert.rejects(jobs(tx).request(def, 1, { key: undefined! }), TypeError);
    await assert.rejects(jobs(tx).request(def, 1, { key: 'x'.repeat(501) }), TypeError);
    await assert.rejects(
      jobs(tx).request(def, 1, { key: 'one', runAt: new Date('invalid') }),
      TypeError,
    );
    await assert.rejects(
      jobs(tx).request(def, 1, { key: 'one', runAt: new Date(), delayMs: 1 }),
      TypeError,
    );
    await assert.rejects(jobs(tx).request(def, undefined!, { key: 'one' }), TypeError);
    assert.equal((await jobs(tx).request(def, 2, { key: '' })).payload, 2);
    await tx.queryClassic('SELECT 1');
  });
});

test('incompatible installations are rejected without upgrading or erasing data', async () => {
  const def = definition();
  const original = await jobs(pool).send(def, 1);
  const rollback = new Error('rollback schema fixture');
  await assert.rejects(
    pool.transaction(async (tx) => {
      await tx.queryClassic('UPDATE pgclear_jobs.schema_version SET version = 99');
      await assert.rejects(
        checkSchema((text, values) => tx.queryClassic(text, values)),
        /schema is incompatible/,
      );
      await assert.rejects(
        installJobs({ transaction: (work) => tx.savepoint(work) }),
        /schema is incompatible/,
      );
      assert.equal((await jobs(tx).get(original.id))?.payload, 1);
      throw rollback;
    }),
    (error) => error === rollback,
  );
  await checkSchema(execute);
});

async function waitingForLock(pid: number) {
  await until(async () => {
    const { rows } = await pool.queryClassic<{ waiting: boolean }>(
      "SELECT wait_event_type = 'Lock' AS waiting FROM pg_stat_activity WHERE pid = $1",
      [pid],
    );
    return rows[0]?.waiting === true;
  });
}

for (const existingSuccessor of [false, true]) {
  test(`a request waiting behind completion follows the promoted head (existing successor: ${existingSuccessor})`, async () => {
    const def = definition();
    const original = await jobs(pool).request(def, 1, { key: 'one' });
    const current = await claim(def.name);
    const successor = existingSuccessor ? await jobs(pool).request(def, 2, { key: 'one' }) : null;
    const session = await pool.acquire();
    const pid = (await session.queryClassic<{ pid: number }>('SELECT pg_backend_pid() AS pid'))
      .rows[0]!.pid;
    let pending: ReturnType<ReturnType<typeof jobs>['request']> | undefined;
    try {
      await pool.transaction(async (tx) => {
        await tx.queryClassic('SELECT id FROM pgclear_jobs.jobs WHERE id = $1 FOR UPDATE', [
          original.id,
        ]);
        pending = jobs(session).request(def, 3, { key: 'one' });
        await waitingForLock(pid);
        assert.equal(
          await finishClaim((text, values) => tx.queryClassic(text, values), current, null),
          true,
        );
      });
      const requested = (await pending)!;
      if (successor) assert.equal(requested.id, successor.id);
      const next = await claim(def.name);
      assert.equal(next.id, requested.id);
      assert.equal(next.blocked_by, null);
      assert.equal(next.payload, 3);
      await finishClaim(execute, next, null);
      assert.equal((await jobs(pool).stats(def)).pending, 0);
    } finally {
      await pending;
      await session.release();
    }
  });
}

test('completion waiting behind a request releases the newly committed successor', async () => {
  const def = definition();
  const original = await jobs(pool).request(def, 1, { key: 'one' });
  const current = await claim(def.name);
  const session = await pool.acquire();
  const pid = (await session.queryClassic<{ pid: number }>('SELECT pg_backend_pid() AS pid'))
    .rows[0]!.pid;
  let finishing: Promise<boolean> | undefined;
  let successorId = '';
  try {
    await pool.transaction(async (tx) => {
      successorId = (await jobs(tx).request(def, 2, { key: 'one' })).id;
      finishing = finishClaim((text, values) => session.queryClassic(text, values), current, null);
      await waitingForLock(pid);
      assert.equal((await jobs(tx).get(successorId))?.blockedBy, original.id);
    });
    assert.equal(await finishing, true);
    assert.equal((await claim(def.name)).id, successorId);
  } finally {
    await finishing;
    await session.release();
  }
});

test('a request waiting behind successor cancellation retains replacement work', async () => {
  const def = definition();
  const original = await jobs(pool).request(def, 1, { key: 'one' });
  const current = await claim(def.name);
  const successor = await jobs(pool).request(def, 2, { key: 'one' });
  const session = await pool.acquire();
  const pid = (await session.queryClassic<{ pid: number }>('SELECT pg_backend_pid() AS pid'))
    .rows[0]!.pid;
  let pending: ReturnType<ReturnType<typeof jobs>['request']> | undefined;
  try {
    await pool.transaction(async (tx) => {
      await tx.queryClassic('SELECT id FROM pgclear_jobs.jobs WHERE id = $1 FOR UPDATE', [
        successor.id,
      ]);
      pending = jobs(session).request(def, 3, { key: 'one' });
      await waitingForLock(pid);
      await jobs(tx).cancel(successor.id);
    });
    const replacement = (await pending)!;
    assert.notEqual(replacement.id, successor.id);
    assert.equal(replacement.blockedBy, original.id);
    assert.equal(replacement.payload, 3);
    await finishClaim(execute, current, null);
    assert.equal((await claim(def.name)).id, replacement.id);
  } finally {
    await pending;
    await session.release();
  }
});
