import assert from 'node:assert/strict';
import { fork } from 'node:child_process';
import { once } from 'node:events';
import { setTimeout as delay } from 'node:timers/promises';
import { test } from 'node:test';
import { jobs, runJobs, defineJob, JobStateError } from '../../src/index.js';
import {
  claimJob,
  finishClaim,
  finishCancellation,
  recoverClaims,
  renewClaim,
} from '../../src/claims.js';
import { definition, execute, pool, until, url } from '../helpers/database.js';
import type { Execute } from '../../src/storage.js';

test('a different process observes durable cancellation through the existing handler signal', async () => {
  const spec = definition();
  const job = await jobs(pool).send(spec, 1);
  const child = fork(new URL('../fixtures/cancellable-worker.js', import.meta.url), [spec.name], {
    env: { ...process.env, PGCLEAR_JOBS_TEST_DATABASE_URL: url },
    silent: true,
  });
  let stderr = '';
  child.stderr?.on('data', (data: Buffer) => {
    stderr += data.toString();
  });
  const messages: unknown[] = [];
  child.on('message', (message) => messages.push(message));
  const exited = once(child, 'exit');
  try {
    await until(async () => messages.includes('started'));
    const requested = await jobs(pool).cancel(job.id);
    assert.equal(requested.state, 'running');
    assert.ok(requested.cancelRequestedAt instanceof Date);
    await until(async () => (await jobs(pool).get(job.id))?.state === 'cancelled');
    assert.ok(messages.includes('aborted'));
    assert.equal((await jobs(pool).attempts(job.id))[0]?.outcome, 'cancelled');
    assert.equal((await jobs(pool).get(job.id))?.attempts, 1);
    assert.equal(
      (await jobs(pool).cancel(job.id)).cancelRequestedAt?.getTime(),
      requested.cancelRequestedAt?.getTime(),
    );
  } finally {
    child.kill('SIGTERM');
    const [code] = await exited;
    assert.equal(code, 0, stderr);
  }
});

for (const delivery of ['heartbeat', 'completion'] as const) {
  test(`transactional cancellation at ${delivery} rolls back writes and promotes one keyed successor`, async () => {
    await pool.queryClassic('CREATE TABLE cancellation_effects (value integer)');
    let entered = false;
    const resume = Promise.withResolvers<void>();
    let failures = 0;
    const cancelled: string[] = [];
    const def = defineJob({
      name: definition().name,
      transactional: true,
      async handler(value: number, { tx, signal }) {
        await tx.queryClassic('INSERT INTO cancellation_effects VALUES ($1)', [value]);
        if (value === 1) {
          entered = true;
          if (delivery === 'heartbeat') await delay(60000, undefined, { signal });
          else await resume.promise;
        }
      },
    });
    const head = await jobs(pool).request(def, 1, { key: 'same' });
    const stop = new AbortController();
    const running = runJobs(pool, [def], {
      signal: stop.signal,
      leaseMs: delivery === 'heartbeat' ? 600 : 30000,
      pollIntervalMs: 5,
      onHandlerError() {
        failures++;
        return 'continue';
      },
      onEvent(event) {
        if (event.type === 'attemptCancelled') cancelled.push(event.id);
      },
    });
    try {
      await until(async () => entered);
      const successor = await jobs(pool).request(def, 2, { key: 'same' });
      await jobs(pool).cancel(head.id);
      resume.resolve();
      await until(async () => (await jobs(pool).get(successor.id))?.state === 'completed');
      assert.equal((await jobs(pool).get(head.id))?.state, 'cancelled');
      assert.deepEqual((await pool.queryClassic('SELECT value FROM cancellation_effects')).rows, [
        { value: 2 },
      ]);
      assert.equal(failures, 0);
      assert.deepEqual(cancelled, [head.id]);
    } finally {
      resume.resolve();
      stop.abort();
      await running;
      await pool.queryClassic('DROP TABLE cancellation_effects');
    }
  });
}

test('recovery cancels a requested abandoned claim without retrying and fences stale completion', async () => {
  const def = definition({ maxAttempts: 5 });
  const job = await jobs(pool).send(def, 1);
  const claim = (await claimJob(execute, [def.name], 30000)).claim!;
  await jobs(pool).cancel(job.id);
  assert.equal(await finishClaim(execute, claim, null), false);
  assert.equal(await finishClaim(execute, claim, 'failed'), false);
  await pool.queryClassic(
    "UPDATE pgclear_jobs.jobs SET lease_until = clock_timestamp() - interval '1 second' WHERE id = $1",
    [job.id],
  );
  await recoverClaims(execute);
  assert.equal((await jobs(pool).get(job.id))?.state, 'cancelled');
  assert.equal((await jobs(pool).attempts(job.id))[0]?.outcome, 'cancelled');
  assert.equal(await finishCancellation(execute, claim), false);
  assert.equal((await claimJob(execute, [def.name], 30000)).claim, null);
});

test('completion and cancellation arbitrate atomically without rewriting a committed result', async () => {
  const def = definition();
  for (let i = 0; i < 12; i++) {
    const job = await jobs(pool).send(def, i);
    const claim = (await claimJob(execute, [def.name], 30000)).claim!;
    const [completion, cancellation] = await Promise.allSettled([
      finishClaim(execute, claim, null),
      jobs(pool).cancel(job.id),
    ]);
    assert.equal(completion.status, 'fulfilled');
    if (completion.status === 'fulfilled' && completion.value) {
      assert.equal(cancellation.status, 'rejected');
      if (cancellation.status === 'rejected')
        assert.ok(cancellation.reason instanceof JobStateError);
      assert.equal((await jobs(pool).get(job.id))?.state, 'completed');
    } else {
      assert.equal(cancellation.status, 'fulfilled');
      assert.equal(await finishCancellation(execute, claim), true);
      assert.equal((await jobs(pool).get(job.id))?.state, 'cancelled');
    }
  }
});

test('an uncooperative cancelled handler keeps its local slot until it settles', async () => {
  const release = Promise.withResolvers<void>();
  let signal: AbortSignal | undefined;
  const seen: number[] = [];
  const def = definition({
    async handler(value, context) {
      seen.push(value);
      if (value === 1) {
        signal = context.signal;
        await release.promise;
      }
    },
  });
  const first = await jobs(pool).send(def, 1);
  await jobs(pool).send(def, 2);
  const stop = new AbortController();
  const running = runJobs(pool, [def], {
    signal: stop.signal,
    leaseMs: 600,
    pollIntervalMs: 5,
    recoveryIntervalMs: 10,
  });
  try {
    await until(async () => signal !== undefined);
    await jobs(pool).cancel(first.id);
    await until(async () => signal!.aborted);
    assert.deepEqual(seen, [1]);
    await until(async () => (await jobs(pool).get(first.id))?.state === 'cancelled');
    assert.deepEqual(seen, [1]);
    release.resolve();
    await until(async () => seen.length === 2);
    assert.deepEqual(seen, [1, 2]);
  } finally {
    release.resolve();
    stop.abort();
    await running;
  }
});

test('cancellation through a rolled-back producer transaction leaves the job runnable', async () => {
  const def = definition();
  const job = await jobs(pool).send(def, 1);
  const claim = (await claimJob(execute, [def.name], 30000)).claim!;
  await assert.rejects(
    pool.transaction(async (tx) => {
      await jobs(tx).cancel(job.id);
      assert.equal((await jobs(pool).get(job.id))?.cancelRequestedAt, null);
      assert.equal(
        await finishClaim((text, values) => tx.queryClassic(text, values), claim, null),
        false,
      );
      throw new Error('rollback');
    }),
    /rollback/,
  );
  assert.equal(await finishClaim(execute, claim, null), true);
});

for (const finalizer of ['worker', 'recovery'] as const) {
  test(`${finalizer} cancellation preserves the previous attempt's failure diagnostic`, async () => {
    const def = definition({ maxAttempts: 3 });
    const job = await jobs(pool).send(def, 1);
    const first = (await claimJob(execute, [def.name], 30000)).claim!;
    await finishClaim(execute, first, 'Earlier provider failure');
    const second = (await claimJob(execute, [def.name], 30000)).claim!;
    await jobs(pool).cancel(job.id);
    if (finalizer === 'worker') assert.equal(await finishCancellation(execute, second), true);
    else {
      await pool.queryClassic(
        "UPDATE pgclear_jobs.jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1",
        [job.id],
      );
      await recoverClaims(execute);
    }
    const record = await jobs(pool).get(job.id);
    assert.equal(record?.state, 'cancelled');
    assert.equal(record?.lastError, 'Earlier provider failure');
    const attempts = await jobs(pool).attempts(job.id);
    assert.deepEqual(
      attempts.map((a) => [a.outcome, a.error]),
      [
        ['cancelled', null],
        ['failed', 'Earlier provider failure'],
      ],
    );
  });
}

for (const operation of ['renew', 'complete'] as const) {
  test(`${operation} observes cancellation committed while waiting for the job row`, async () => {
    const def = definition();
    const job = await jobs(pool).send(def, 1);
    const claim = (await claimJob(execute, [def.name], 30000)).claim!;
    const session = await pool.acquire();
    let pending: Promise<boolean | 'cancelled'> | undefined;
    try {
      await pool.transaction(async (tx) => {
        await tx.queryClassic('SELECT id FROM pgclear_jobs.jobs WHERE id=$1 FOR UPDATE', [job.id]);
        const contender: Execute = (text, values) => session.queryClassic(text, values);
        pending =
          operation === 'renew'
            ? renewClaim(contender, claim, 30000)
            : finishClaim(contender, claim, null);
        await until(async () => {
          const { rows } = await pool.queryClassic<{ blocked: boolean }>(
            'SELECT cardinality(pg_blocking_pids($1)) > 0 AS blocked',
            [session.info.backendPid],
          );
          return rows[0]?.blocked === true;
        });
        await jobs(tx).cancel(job.id);
      });
      assert.equal(await pending, operation === 'renew' ? 'cancelled' : false);
      assert.equal((await jobs(pool).get(job.id))?.state, 'running');
      assert.equal(await finishCancellation(execute, claim), true);
    } finally {
      await pending;
      await session.release();
    }
  });
}
