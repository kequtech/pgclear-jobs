import assert from 'node:assert/strict';
import test from 'node:test';
import { setTimeout as delay } from 'node:timers/promises';
import { createTenon } from '@kequtech/tenon-orm';
import { defineJob, JobError, jobs, runJobs } from '../../src/index.js';
import { jobs as tenonJobs } from '../../src/tenon.js';
import { claimJob, finishClaim, recoverClaims } from '../../src/claims.js';
import { advanceSchedules } from '../../src/schedules.js';
import { definition, execute, pool, until, url } from '../helpers/database.js';

test('one retry policy provides exponential backoff, a cap and the same behavior on recovery', async () => {
  const def = definition({ maxAttempts: 6, retryDelayMs: 20, retryMaxDelayMs: 50 });
  const row = await jobs(pool).send(def, 1);
  for (const expected of [20, 40, 50]) {
    const { claim } = await claimJob(execute, [def.name], 30000);
    assert.ok(claim);
    await finishClaim(execute, claim, 'temporary failure');
    const retried = (await jobs(pool).get(row.id))!;
    assert.ok(Math.abs(retried.runAt.getTime() - retried.updatedAt.getTime() - expected) <= 5);
    await jobs(pool).reschedule(row.id, new Date(0));
  }
  const { claim } = await claimJob(execute, [def.name], 30000);
  assert.ok(claim);
  await pool.queryClassic(
    "UPDATE tenon_jobs.jobs SET lease_until = clock_timestamp() - interval '1 second' WHERE id = $1",
    [row.id],
  );
  await recoverClaims(execute);
  const recovered = (await jobs(pool).get(row.id))!;
  assert.ok(Math.abs(recovered.runAt.getTime() - recovered.updatedAt.getTime() - 50) <= 5);
});

test('optional jitter remains within half to all of the capped backoff', async () => {
  const def = definition({ retryDelayMs: 1000, retryMaxDelayMs: 800, retryJitter: true });
  const row = await jobs(pool).send(def, 1);
  const { claim } = await claimJob(execute, [def.name], 30000);
  assert.ok(claim);
  await finishClaim(execute, claim, 'temporary failure');
  const retried = (await jobs(pool).get(row.id))!;
  const ms = retried.runAt.getTime() - retried.updatedAt.getTime();
  assert.ok(ms >= 395 && ms <= 805);
});

test('retryAt respects backoff and the attempt budget, and permanent failures stop immediately', async () => {
  const def = definition({ maxAttempts: 2, retryDelayMs: 200 });
  const row = await jobs(pool).send(def, 1);
  const first = (await claimJob(execute, [def.name], 30000)).claim!;
  const at = new Date(Date.now() + 60_000);
  await finishClaim(execute, first, new JobError('provider throttled', { retryAt: at }));
  assert.equal((await jobs(pool).get(row.id))?.runAt.getTime(), at.getTime());
  await jobs(pool).reschedule(row.id, new Date(0));
  const second = (await claimJob(execute, [def.name], 30000)).claim!;
  await finishClaim(execute, second, new JobError('still throttled', { retryAt: at }));
  assert.equal((await jobs(pool).get(row.id))?.state, 'failed');
  const permanent = await jobs(pool).send(def, 2);
  const claim = (await claimJob(execute, [def.name], 30000)).claim!;
  await finishClaim(execute, claim, new JobError('invalid recipient', { retry: false }));
  assert.equal((await jobs(pool).get(permanent.id))?.attempts, 1);
  assert.equal((await jobs(pool).get(permanent.id))?.state, 'failed');
  const backoff = await jobs(pool).send(def, 3);
  await finishClaim(
    execute,
    (await claimJob(execute, [def.name], 30000)).claim!,
    new JobError('retry too soon', { retryAt: new Date(0) }),
  );
  const pending = (await jobs(pool).get(backoff.id))!;
  assert.ok(pending.runAt.getTime() - pending.updatedAt.getTime() >= 195);
});

test('handlers use persisted retry policy and need no manual retry scheduling', async () => {
  const contexts: { attempt: number; maxAttempts: number }[] = [];
  let retryAt = 0;
  const def = definition({
    maxAttempts: 2,
    retryDelayMs: 10,
    handler(_payload, context) {
      contexts.push(context);
      if (context.attempt === 1) {
        retryAt = Date.now() + 100;
        throw new JobError('rate limited', { retryAt: new Date(retryAt) });
      }
      assert.ok(Date.now() >= retryAt);
    },
  });
  const row = await jobs(pool).send(def, 1);
  const deployed = defineJob({ ...def, maxAttempts: 10 });
  const stop = new AbortController();
  const running = runJobs(pool, [deployed], { signal: stop.signal, shutdownTimeoutMs: 20 });
  try {
    await until(async () => (await jobs(pool).get(row.id))?.state === 'completed');
  } finally {
    stop.abort();
    await running;
  }
  assert.deepEqual(
    contexts.map(({ attempt, maxAttempts }) => ({ attempt, maxAttempts })),
    [
      { attempt: 1, maxAttempts: 2 },
      { attempt: 2, maxAttempts: 2 },
    ],
  );
});

test('delayMs uses the database clock and validates conflicting timing options', async () => {
  const def = definition();
  const row = await jobs(pool).send(def, 1, { delayMs: 200 });
  assert.ok(Math.abs(row.runAt.getTime() - row.createdAt.getTime() - 200) <= 5);
  assert.equal((await claimJob(execute, [def.name], 30000)).claim, null);
  await assert.rejects(jobs(pool).send(def, 1, { delayMs: -1 }), TypeError);
  await assert.rejects(jobs(pool).send(def, 1, { delayMs: 1, runAt: new Date() }), TypeError);
});

test('per-definition concurrency holds its own slots without blocking other definitions', async () => {
  const release = Promise.withResolvers<void>();
  let slowActive = 0,
    fastActive = 0;
  const slow = definition({
    concurrency: 1,
    async handler() {
      slowActive++;
      try {
        await release.promise;
      } finally {
        slowActive--;
      }
    },
  });
  const fast = definition({
    concurrency: 2,
    async handler() {
      fastActive++;
      try {
        await release.promise;
      } finally {
        fastActive--;
      }
    },
  });
  await Promise.all([1, 2, 3].map((n) => jobs(pool).send(slow, n)));
  await Promise.all([1, 2, 3].map((n) => jobs(pool).send(fast, n)));
  const stop = new AbortController();
  const running = runJobs(pool, [slow, fast], {
    signal: stop.signal,
    concurrency: 5,
    shutdownTimeoutMs: 20,
  });
  try {
    await until(async () => slowActive === 1 && fastActive === 2);
    await delay(40);
    assert.equal(slowActive, 1);
    assert.equal(fastActive, 2);
  } finally {
    stop.abort();
    release.resolve();
    await running;
  }
});

async function due(id: string): Promise<void> {
  await pool.queryClassic(
    "UPDATE tenon_jobs.schedules SET next_run_at = '2000-01-01T00:00:00Z' WHERE id = $1",
    [id],
  );
}

test('schedule registration is idempotent and preserves its cursor across unchanged restarts', async () => {
  const def = definition();
  const first = await jobs(pool).schedule(def, 1, { cron: '* * * * *', key: 'regular' });
  await due(first.id);
  const second = await jobs(pool).schedule(def, 2, { cron: '* * * * *', key: 'regular' });
  assert.equal(first.id, second.id);
  assert.equal(second.nextRunAt.toISOString(), '2000-01-01T00:00:00.000Z');
  assert.equal(second.payload, 2);
  assert.equal((await jobs(pool).schedules(def)).length, 1);
  const updated = await jobs(pool).schedule(def, 3, { cron: '0 * * * *', key: 'regular' });
  assert.ok(updated.nextRunAt.getTime() > Date.now());
  assert.equal(await jobs(pool).unschedule(def, 'regular'), true);
  assert.equal(await jobs(pool).unschedule(def, 'regular'), false);
});

test('schedule operations join pgclear and Tenon application transactions', async () => {
  const def = definition();
  await assert.rejects(
    pool.transaction(async (tx) => {
      await jobs(tx).schedule(def, 1, { cron: '* * * * *' });
      assert.equal((await jobs(pool).schedules(def)).length, 0);
      throw new Error('rollback');
    }),
    /rollback/,
  );
  const db = createTenon({ connection: url });
  try {
    await db.transaction((tx) => tenonJobs(tx).schedule(def, 2, { cron: '* * * * *' }));
    assert.equal((await jobs(pool).schedules(def))[0]?.payload, 2);
    await assert.rejects(
      db.transaction(async (tx) => {
        await tenonJobs(tx).unschedule(def);
        throw new Error('rollback');
      }),
      /rollback/,
    );
    assert.equal((await jobs(pool).schedules(def)).length, 1);
  } finally {
    await db.close();
  }
});

test('competing schedule passes create one occurrence and retain its identity after completion', async () => {
  const def = definition();
  const schedule = await jobs(pool).schedule(def, 42, { cron: '* * * * *' });
  await due(schedule.id);
  await Promise.all(Array.from({ length: 8 }, () => advanceSchedules(pool, [def.name], 5000)));
  const work = await jobs(pool).find(def);
  assert.equal(work.length, 1);
  assert.equal(work[0]?.payload, 42);
  assert.equal(work[0]?.scheduleId, schedule.id);
  assert.ok(work[0]?.scheduledFor instanceof Date);
  assert.equal(work[0]?.scheduledFor?.toISOString(), '2000-01-01T00:00:00.000Z');
  assert.equal((await jobs(pool).schedules(def))[0]?.lastJobId, work[0]?.id);
  await jobs(pool).cancel(work[0]!.id);
  await due(schedule.id);
  await advanceSchedules(pool, [def.name], 5000);
  assert.equal((await jobs(pool).find(def)).length, 1);
});

test('schedules skip outstanding work and resume after it terminates', async () => {
  const def = definition();
  const schedule = await jobs(pool).schedule(def, 42, { cron: '* * * * *' });
  const old = await jobs(pool).send(def, 41);
  await pool.queryClassic(
    "UPDATE tenon_jobs.jobs SET schedule_id = $2, scheduled_for = '1999-01-01T00:00:00Z' WHERE id = $1",
    [old.id, schedule.id],
  );
  await due(schedule.id);
  assert.equal(await advanceSchedules(pool, [def.name], 5000), 0);
  await jobs(pool).cancel(old.id);
  await due(schedule.id);
  assert.equal(await advanceSchedules(pool, [def.name], 5000), 1);
  assert.equal((await jobs(pool).find(def, { states: ['pending'] })).length, 1);
});

test('downtime produces one catch-up occurrence without replaying every missed tick', async () => {
  const def = definition();
  const month = ((new Date().getUTCMonth() + 6) % 12) + 1;
  const cron = `0 0 1 ${month} *`;
  const once = await jobs(pool).schedule(def, 2, { cron, key: 'once' });
  await due(once.id);
  assert.equal(await advanceSchedules(pool, [def.name], 5000), 1);
  const work = await jobs(pool).find(def);
  assert.equal(work.length, 1);
  assert.equal(work[0]?.scheduleId, once.id);
  assert.ok((await jobs(pool).schedules(def)).every((s) => s.nextRunAt.getTime() > Date.now()));
});

test('schedule jobs and cursor advancement roll back together', async () => {
  const def = definition();
  const schedule = await jobs(pool).schedule(def, 1, { cron: '* * * * *' });
  await due(schedule.id);
  await assert.rejects(
    pool.transaction(async (tx) => {
      await advanceSchedules(
        { queryClassic: tx.queryClassic.bind(tx), transaction: (work) => tx.savepoint(work) },
        [def.name],
        5000,
      );
      assert.equal((await jobs(tx).find(def)).length, 1);
      assert.ok((await jobs(tx).schedules(def))[0]!.nextRunAt.getTime() > Date.now());
      throw new Error('rollback');
    }),
    /rollback/,
  );
  assert.equal((await jobs(pool).find(def)).length, 0);
  assert.equal(
    (await jobs(pool).schedules(def))[0]?.nextRunAt.toISOString(),
    '2000-01-01T00:00:00.000Z',
  );
});

test('runners turn only their selected schedules into ordinary executable jobs', async () => {
  let scheduledFor: Date | null = null;
  const def = definition({
    handler(_payload, context) {
      scheduledFor = context.scheduledFor;
    },
  });
  const unknown = definition();
  const knownSchedule = await jobs(pool).schedule(def, 1, { cron: '* * * * *' });
  const otherSchedule = await jobs(pool).schedule(unknown, 2, { cron: '* * * * *' });
  await due(knownSchedule.id);
  await due(otherSchedule.id);
  const stop = new AbortController();
  const running = runJobs(pool, [def], {
    signal: stop.signal,
    pollIntervalMs: 30000,
    recoveryIntervalMs: 10,
    shutdownTimeoutMs: 20,
  });
  try {
    await until(async () => (await jobs(pool).find(def, { states: ['completed'] })).length === 1);
  } finally {
    stop.abort();
    await running;
  }
  assert.ok(scheduledFor);
  assert.equal((await jobs(pool).find(unknown)).length, 0);
});
