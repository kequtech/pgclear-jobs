import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import test from 'node:test';
import { createTenon } from '@kequtech/tenon-orm';
import { defineJob, installJobs, jobs, JobNotFoundError, JobStateError } from '../../src/index.js';
import { jobs as tenonJobs } from '../../src/tenon.js';
import { claimJob, finishClaim } from '../../src/claims.js';
import { advanceSchedules } from '../../src/schedules.js';
import { definition, execute, names, pool, url } from '../helpers/database.js';

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

test('bulk submission skips duplicate IDs and keys and preserves null JSON payloads', async () => {
  const def = defineJob({ name: definition().name, handler(_payload: unknown) {} });
  const id = randomUUID();
  const repeated = randomUUID();
  const existing = await jobs(pool).send(def, 1, { id });
  const inserted = await jobs(pool).sendMany(def, [
    { id, payload: 'ignored' },
    { id: randomUUID(), payload: null },
    { id: repeated, payload: 'first' },
    { id: repeated, payload: 'second' },
    { key: 'one', payload: { value: 2 } },
    { key: 'one', payload: { value: 3 } },
    { payload: false, delayMs: 60000 },
  ]);
  assert.equal(inserted.length, 4);
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
    assert.equal((await jobs(tx).find(def)).length, 5);
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

test('batch submission and pending upserts join the supplied Tenon transaction', async () => {
  const def = definition();
  const db = createTenon({ connection: url });
  try {
    await assert.rejects(
      db.transaction(async (tx) => {
        const scope = tenonJobs(tx);
        await scope.sendMany(def, [{ payload: 1 }, { payload: 2 }]);
        await scope.upsert(def, 3, { key: 'pending' });
        await scope.upsert(def, 4, { key: 'pending', delayMs: 1000 });
        assert.equal((await scope.stats(def)).pending, 3);
        assert.equal((await jobs(pool).find(def)).length, 0);
        throw new Error('rollback');
      }),
      /rollback/,
    );
    assert.equal((await jobs(pool).find(def)).length, 0);
    await db.transaction((tx) => tenonJobs(tx).sendMany(def, [{ payload: 5 }, { payload: 6 }]));
    assert.equal((await jobs(pool).find(def)).length, 2);
  } finally {
    await db.close();
  }
});

test('pending upserts coalesce concurrent submissions and preserve the saved policy and attempt count', async () => {
  const def = definition({ maxAttempts: 3 });
  const submitted = await Promise.all(
    Array.from({ length: 8 }, (_, payload) => jobs(pool).upsert(def, payload, { key: 'one' })),
  );
  assert.equal(new Set(submitted.map((job) => job.id)).size, 1);
  const first = submitted[0]!;
  await finishClaim(execute, (await claimJob(execute, [def.name], 30000)).claim!, 'temporary');
  const changed = defineJob({ ...def, maxAttempts: 8 });
  const updated = await jobs(pool).upsert(changed, 99, { key: 'one', delayMs: 60000 });
  assert.equal(updated.id, first.id);
  assert.equal(updated.payload, 99);
  assert.equal(updated.attempts, 1);
  assert.equal(updated.policy.maxAttempts, 3);
  assert.ok(updated.runAt.getTime() > Date.now() + 59000);
  const payloadOnly = await jobs(pool).upsert(def, 100, { key: 'one' });
  assert.equal(payloadOnly.runAt.getTime(), updated.runAt.getTime());
  assert.equal((await jobs(pool).send(def, 101, { key: 'one' })).payload, 100);
});

test('an upsert racing an active claim reports a usable application conflict', async () => {
  const def = definition();
  const original = await jobs(pool).send(def, 1, { key: 'one' });
  const claim = (await claimJob(execute, [def.name], 30000)).claim!;
  await pool.transaction(async (tx) => {
    await assert.rejects(jobs(tx).upsert(def, 2, { key: 'one' }), JobStateError);
    await tx.queryClassic('SELECT 1');
  });
  assert.equal((await jobs(pool).get(original.id))?.payload, 1);
  await finishClaim(execute, claim, null);
  assert.notEqual((await jobs(pool).upsert(def, 3, { key: 'one' })).id, original.id);
});

test('manual retry preserves failed history and returns a fresh linked job and budget', async () => {
  const def = definition({ maxAttempts: 2 });
  const failed = await jobs(pool).send(def, 42, { key: 'one' });
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
    "UPDATE tenon_jobs.schedules SET next_run_at = '2000-01-01' WHERE id = $1",
    [schedule.id],
  );
  await advanceSchedules(pool, [def.name], 5000);
  const original = (await jobs(pool).find(def))[0]!;
  await finishClaim(execute, (await claimJob(execute, [def.name], 30000)).claim!, 'failed');
  const retry = await jobs(pool).retry(original.id);
  assert.equal(retry.scheduleId, schedule.id);
  assert.deepEqual(retry.scheduledFor, original.scheduledFor);
  await pool.queryClassic(
    "UPDATE tenon_jobs.schedules SET next_run_at = '2000-01-02' WHERE id = $1",
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
      "UPDATE tenon_jobs.jobs SET created_at = '2026-01-01T00:00:00Z'::timestamptz + $2 * interval '100 microseconds' WHERE id = $1",
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
    "UPDATE tenon_jobs.jobs SET finished_at = '1999-01-01' WHERE name = $1 AND state <> 'pending'",
    [def.name],
  );
  const before = new Date('2000-01-01');
  await pool.transaction(async (tx) => {
    await tx.queryClassic('SELECT id FROM tenon_jobs.jobs WHERE id = $1 FOR UPDATE', [
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
    total: 6,
  });
});

test('upgrading version 3 preserves existing jobs, schedules and saved policy', async () => {
  const def = definition();
  await assert.rejects(
    pool.transaction(async (tx) => {
      const job = await jobs(tx).send(def, 42);
      const schedule = await jobs(tx).schedule(def, 43, { cron: '0 * * * *' });
      // Earlier tests created v4-only occurrence retries; they cannot exist in a v3 fixture.
      await tx.queryClassic(
        'DELETE FROM tenon_jobs.jobs WHERE retry_of IS NOT NULL AND name IN (SELECT jsonb_array_elements_text($1::jsonb))',
        [JSON.stringify(names)],
      );
      await tx.queryClassic('DROP INDEX tenon_jobs.jobs_terminal');
      await tx.queryClassic('DROP INDEX tenon_jobs.jobs_schedule_occurrence');
      await tx.queryClassic('ALTER TABLE tenon_jobs.jobs DROP COLUMN retry_of');
      await tx.queryClassic(
        'CREATE UNIQUE INDEX jobs_schedule_occurrence ON tenon_jobs.jobs (schedule_id, scheduled_for)',
      );
      await tx.queryClassic('DELETE FROM tenon_jobs.migrations WHERE version = 4');
      await installJobs({ transaction: (work) => tx.savepoint(work) });
      assert.deepEqual(await jobs(tx).get(job.id), job);
      assert.deepEqual((await jobs(tx).schedules(def))[0], schedule);
      throw new Error('rollback upgrade fixture');
    }),
    /rollback upgrade fixture/,
  );
});
