import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import test from 'node:test';
import { defineJob, jobs, JobNotFoundError, JobStateError } from '../../src/index.js';
import { claimJob, finishClaim } from '../../src/claims.js';
import { advanceSchedules } from '../../src/schedules.js';
import { definition, execute, pool } from '../helpers/database.js';

test('stable submission IDs preserve the first job across completion and concurrent retries', async () => {
  const def = definition();
  const id = randomUUID();
  const results = await Promise.all(
    Array.from({ length: 8 }, (_, n) => jobs(pool).send(def, n, { id })),
  );
  assert.ok(results.every((job) => job.id === id && job.payload === results[0]!.payload));
  await finishClaim(execute, (await claimJob(execute, [def.name], 30000)).claim!, null);
  const again = await jobs(pool).send(def, 99, { id: id.toUpperCase() });
  assert.equal(again.state, 'completed');
  assert.equal(again.payload, results[0]!.payload);
  await pool.transaction(async (tx) => {
    await assert.rejects(jobs(tx).send(definition(), 1, { id }), JobStateError);
    await assert.rejects(jobs(tx).send(def, 1, { id: 'invalid' }), TypeError);
    // @ts-expect-error Identity and coalescing cannot be combined at runtime either.
    await assert.rejects(jobs(tx).send(def, 1, { id, key: 'same' }), TypeError);
    await tx.queryClassic('SELECT 1');
  });
  assert.equal(await jobs(pool).remove(id), true);
  const recreated = await jobs(pool).send(def, 99, { id });
  assert.equal(recreated.state, 'pending');
  assert.equal(recreated.payload, 99);
});

test('bulk submission skips duplicate IDs and preserves null JSON payloads', async () => {
  const def = defineJob({ name: definition().name, handler(_payload: unknown) {} });
  const id = randomUUID();
  const repeated = randomUUID();
  const existing = await jobs(pool).send(def, 1, { id });
  const inserted = await jobs(pool).sendMany(def, [
    { id, payload: 'ignored' },
    { id: randomUUID(), payload: null },
    { id: repeated, payload: 'first' },
    { id: repeated, payload: 'second' },
    { payload: { value: 2 } },
    { payload: { value: 2 } },
    { payload: false, delayMs: 60000 },
  ]);
  assert.equal(inserted.length, 5);
  assert.ok(inserted.some((job) => job.payload === null));
  assert.equal((await jobs(pool).get(existing.id))?.payload, 1);
  const delayed = inserted.find((job) => job.payload === false)!;
  assert.ok(Math.abs(delayed.runAt.getTime() - delayed.createdAt.getTime() - 60000) < 10);
  assert.deepEqual(await jobs(pool).sendMany(def, []), []);
  await pool.transaction(async (tx) => {
    await assert.rejects(
      jobs(tx).sendMany(def, [{ payload: 'valid' }, { payload: undefined }]),
      TypeError,
    );
    await assert.rejects(jobs(tx).sendMany(def, new Array<{ payload: unknown }>(1)), TypeError);
    assert.equal((await jobs(tx).find(def)).length, 6);
    await assert.rejects(
      jobs(tx).sendMany(
        def,
        Array.from({ length: 1001 }, () => ({ payload: null })),
      ),
      TypeError,
    );
    await tx.queryClassic('SELECT 1');
  });
});

test('batch submission and keyed requests join the supplied transaction', async () => {
  const def = definition();
  await assert.rejects(
    pool.transaction(async (tx) => {
      const scope = jobs(tx);
      await scope.sendMany(def, [{ payload: 1 }, { payload: 2 }]);
      await scope.request(def, 3, { key: 'pending' });
      await scope.request(def, 4, { key: 'pending', delayMs: 1000 });
      assert.equal((await scope.stats(def)).pending, 3);
      assert.equal((await jobs(pool).find(def)).length, 0);
      throw new Error('rollback');
    }),
    /rollback/,
  );
  assert.equal((await jobs(pool).find(def)).length, 0);
  await pool.transaction((tx) => jobs(tx).sendMany(def, [{ payload: 5 }, { payload: 6 }]));
  assert.equal((await jobs(pool).find(def)).length, 2);
});

test('explicit pending edits preserve a retry job policy, budget and history', async () => {
  const def = definition({ maxAttempts: 3 });
  const first = await jobs(pool).send(def, 1);
  await finishClaim(execute, (await claimJob(execute, [def.name], 30000)).claim!, 'temporary');
  const changed = defineJob({ ...def, maxAttempts: 8 });
  const updated = await jobs(pool).update(changed, first.id, 99);
  assert.equal(updated.id, first.id);
  assert.equal(updated.payload, 99);
  assert.equal(updated.attempts, 1);
  assert.equal(updated.policy.maxAttempts, 3);
  const later = new Date(Date.now() + 60_000);
  const rescheduled = await jobs(pool).reschedule(first.id, later);
  assert.equal(rescheduled.runAt.getTime(), later.getTime());
  assert.equal((await jobs(pool).attempts(first.id)).length, 1);
});

test('manual retry preserves failed history and returns a fresh linked job and budget', async () => {
  const def = definition({ maxAttempts: 2 });
  const failed = await jobs(pool).request(def, 42, { key: 'one' });
  for (let n = 0; n < 2; n++) {
    await finishClaim(execute, (await claimJob(execute, [def.name], 30000)).claim!, 'failed');
  }
  const retry = await jobs(pool).retry(failed.id);
  assert.notEqual(retry.id, failed.id);
  assert.equal(retry.retryOf, failed.id);
  assert.equal(retry.payload, 42);
  assert.equal(retry.key, 'one');
  assert.equal(retry.attempts, 0);
  assert.equal(retry.policy.maxAttempts, 2);
  assert.equal((await jobs(pool).get(failed.id))?.state, 'failed');
  assert.equal((await jobs(pool).attempts(failed.id)).length, 2);
  await pool.transaction(async (tx) => {
    await assert.rejects(jobs(tx).retry(failed.id), JobStateError);
    await assert.rejects(jobs(tx).retry(retry.id), JobStateError);
    await assert.rejects(jobs(tx).retry(randomUUID()), JobNotFoundError);
    await tx.queryClassic('SELECT 1');
  });
  const claim = (await claimJob(execute, [def.name], 30000)).claim!;
  assert.equal(claim.attempt_count, 1);
  await finishClaim(execute, claim, null);
});

test('retrying a scheduled failure preserves occurrence context and overlap protection', async () => {
  const def = definition({ maxAttempts: 1 });
  const schedule = await jobs(pool).schedule(def, 1, { cron: '* * * * *' });
  await pool.queryClassic(
    "UPDATE pgclear_jobs.schedules SET next_run_at = '2000-01-01' WHERE id = $1",
    [schedule.id],
  );
  await advanceSchedules(pool, [def.name], 5000);
  const original = (await jobs(pool).find(def))[0]!;
  await finishClaim(execute, (await claimJob(execute, [def.name], 30000)).claim!, 'failed');
  const retry = await jobs(pool).retry(original.id);
  assert.equal(retry.scheduleId, schedule.id);
  assert.deepEqual(retry.scheduledFor, original.scheduledFor);
  await pool.queryClassic(
    "UPDATE pgclear_jobs.schedules SET next_run_at = '2000-01-02' WHERE id = $1",
    [schedule.id],
  );
  assert.equal(await advanceSchedules(pool, [def.name], 5000), 0);
  assert.equal((await jobs(pool).find(def)).length, 2);
});

test('cursor pagination retains microseconds and works after the cursor row is removed', async () => {
  const def = definition();
  const inserted = await jobs(pool).sendMany(
    def,
    [1, 2, 3, 4].map((payload) => ({ payload })),
  );
  for (const [i, job] of inserted.entries()) {
    await pool.queryClassic(
      "UPDATE pgclear_jobs.jobs SET created_at = '2026-01-01T00:00:00Z'::timestamptz + $2 * interval '100 microseconds' WHERE id = $1",
      [job.id, i],
    );
  }
  const expected = await jobs(pool).find(def);
  assert.equal(new Set(expected.map((job) => job.createdAt.getTime())).size, 1);
  const first = (await jobs(pool).find(def, { limit: 1 }))[0]!;
  await jobs(pool).cancel(first.id);
  await jobs(pool).remove(first.id);
  const ids = [first.id];
  let after = first.cursor;
  for (;;) {
    const page = await jobs(pool).find(def, { limit: 1, after });
    if (!page.length) break;
    ids.push(page[0]!.id);
    after = page[0]!.cursor;
  }
  assert.deepEqual(
    ids,
    expected.map((job) => job.id),
  );
  await pool.transaction(async (tx) => {
    await assert.rejects(jobs(tx).find(def, { after: 'invalid' }), TypeError);
    for (const time of ['2026-02-30T00:00:00Z', '2026-01-01T00:00:00+16:00']) {
      const after = Buffer.from(JSON.stringify([time, first.id])).toString('base64url');
      await assert.rejects(jobs(tx).find(def, { after }), TypeError);
    }
    await tx.queryClassic('SELECT 1');
  });
});

test('pruning is bounded, skips locked rows, preserves failed jobs by default and cascades history', async () => {
  const def = definition({ maxAttempts: 1 });
  const completed = await jobs(pool).send(def, 1);
  await finishClaim(execute, (await claimJob(execute, [def.name], 30000)).claim!, null);
  const cancelled = await jobs(pool).send(def, 2);
  await jobs(pool).cancel(cancelled.id);
  const failed = await jobs(pool).send(def, 3);
  await finishClaim(execute, (await claimJob(execute, [def.name], 30000)).claim!, 'failure');
  const pending = await jobs(pool).send(def, 4);
  await pool.queryClassic(
    "UPDATE pgclear_jobs.jobs SET finished_at = '1999-01-01' WHERE name = $1 AND state <> 'pending'",
    [def.name],
  );
  const before = new Date('2000-01-01');
  await pool.transaction(async (tx) => {
    await tx.queryClassic('SELECT id FROM pgclear_jobs.jobs WHERE id = $1 FOR UPDATE', [
      completed.id,
    ]);
    assert.equal(await jobs(pool).prune(def, { before, limit: 1 }), 1);
    assert.equal(await jobs(pool).get(cancelled.id), null);
    assert.ok(await jobs(pool).get(completed.id));
  });
  assert.equal(await jobs(pool).prune(def, { before }), 1);
  assert.deepEqual(await jobs(pool).attempts(completed.id), []);
  assert.ok(await jobs(pool).get(failed.id));
  assert.ok(await jobs(pool).get(pending.id));
  assert.equal(await jobs(pool).prune(def, { before, states: ['failed'] }), 1);
  assert.equal(await jobs(pool).prune(def, { before }), 0);
  await assert.rejects(jobs(pool).remove(pending.id), JobStateError);
  assert.equal(await jobs(pool).remove(failed.id), false);
});

test('terminal removal and pruning obey transaction rollback', async () => {
  const def = definition();
  const row = await jobs(pool).send(def, 1);
  await jobs(pool).cancel(row.id);
  await assert.rejects(
    pool.transaction(async (tx) => {
      assert.equal(await jobs(tx).remove(row.id), true);
      assert.equal(await jobs(tx).get(row.id), null);
      throw new Error('rollback');
    }),
    /rollback/,
  );
  assert.ok(await jobs(pool).get(row.id));
  await assert.rejects(
    pool.transaction(async (tx) => {
      assert.equal(await jobs(tx).prune(def, { before: new Date(Date.now() + 1000) }), 1);
      throw new Error('rollback');
    }),
    /rollback/,
  );
  assert.ok(await jobs(pool).get(row.id));
});

test('stats distinguish ready, delayed, active and terminal work from a single snapshot', async () => {
  const def = definition({ maxAttempts: 1 });
  const zero = {
    pending: 0,
    running: 0,
    completed: 0,
    failed: 0,
    cancelled: 0,
    ready: 0,
    delayed: 0,
    blocked: 0,
    total: 0,
  };
  assert.deepEqual(await jobs(pool).stats(def), zero);
  await jobs(pool).send(def, 1);
  await finishClaim(execute, (await claimJob(execute, [def.name], 30000)).claim!, null);
  await jobs(pool).send(def, 2);
  await finishClaim(execute, (await claimJob(execute, [def.name], 30000)).claim!, 'failure');
  await jobs(pool).cancel((await jobs(pool).send(def, 3)).id);
  await jobs(pool).send(def, 4);
  await claimJob(execute, [def.name], 30000);
  await jobs(pool).send(def, 5);
  await jobs(pool).send(def, 6, { delayMs: 60000 });
  assert.deepEqual(await jobs(pool).stats(def), {
    pending: 2,
    running: 1,
    completed: 1,
    failed: 1,
    cancelled: 1,
    ready: 1,
    delayed: 1,
    blocked: 0,
    total: 6,
  });
});
