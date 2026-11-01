import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import test from 'node:test';
import { defineJob, jobs, runJobs } from '../../src/index.js';
import { claimJob, finishClaim } from '../../src/claims.js';
import { pruneCompleted } from '../../src/retention.js';
import type { JobDefinition } from '../../src/definition.js';
import { definition, execute, pool, until } from '../helpers/database.js';

async function terminal(
  def: JobDefinition<number>,
  outcome: 'completed' | 'cancelled' | 'failed',
  daysOld = 10,
) {
  const job = await jobs(pool).send(def, 1);
  if (outcome === 'cancelled') await jobs(pool).cancel(job.id);
  else {
    const current = (await claimJob(execute, [def.name], 30_000)).claim!;
    assert.equal(current.id, job.id);
    await finishClaim(execute, current, outcome === 'failed' ? 'permanent' : null);
  }
  await pool.queryClassic(
    "UPDATE pgclear_jobs.jobs SET finished_at = clock_timestamp() - $2::double precision * interval '1 day' WHERE id = $1",
    [job.id, daysOld],
  );
  return job.id;
}

test('the native entry point runs transactional events, keyed requests and cron work with default workers', async () => {
  const handled: { value: number; attempt: number }[] = [];
  const def = defineJob({
    name: definition().name,
    retryDelayMs: 10,
    handler(value: number, { attempt }) {
      handled.push({ value, attempt });
      if (value === 1 && attempt === 1) throw new Error('retry once');
    },
  });
  await pool.transaction(async (tx) => {
    await jobs(tx).send(def, 1);
    await jobs(tx).request(def, 2, { key: 'entity' });
    await jobs(tx).request(def, 3, { key: 'entity' });
  });
  const schedule = await jobs(pool).schedule(def, 4, { cron: '* * * * *' });
  await pool.queryClassic(
    "UPDATE pgclear_jobs.schedules SET next_run_at = '2000-01-01' WHERE id = $1",
    [schedule.id],
  );
  const stop = new AbortController();
  const running = runJobs(pool, [def], { signal: stop.signal });
  try {
    await until(async () => (await jobs(pool).stats(def)).completed === 3);
    assert.deepEqual(
      handled.filter(({ value }) => value === 1).map(({ attempt }) => attempt),
      [1, 2],
    );
    assert.deepEqual(
      handled
        .filter(({ value }) => value !== 1)
        .map(({ value }) => value)
        .sort(),
      [3, 4],
    );
  } finally {
    stop.abort();
    await running;
  }
});

test('default retention removes old completed/cancelled history and preserves failures and outstanding work', async () => {
  const def = definition({ maxAttempts: 1 });
  const completed = await terminal(def, 'completed');
  const cancelled = await terminal(def, 'cancelled');
  const failed = await terminal(def, 'failed', 100);
  const recent = await terminal(def, 'completed', 1);
  const other = await terminal(definition(), 'completed');
  const pending = await jobs(pool).send(def, 2, { delayMs: 86_400_000 });
  const head = await jobs(pool).request(def, 3, { key: 'entity' });
  await claimJob(execute, [def.name], 30_000);
  const successor = await jobs(pool).request(def, 4, { key: 'entity' });
  const marker = await jobs(pool).send(def, 5);
  const stop = new AbortController();
  const running = runJobs(pool, [def], { signal: stop.signal });
  try {
    await until(
      async () =>
        (await jobs(pool).get(completed)) === null && (await jobs(pool).get(cancelled)) === null,
    );
    await until(async () => (await jobs(pool).get(marker.id))?.state === 'completed');
    assert.deepEqual(await jobs(pool).attempts(completed), []);
    assert.equal((await jobs(pool).attempts(failed)).length, 1);
    assert.equal((await jobs(pool).get(failed))?.state, 'failed');
    assert.equal((await jobs(pool).get(recent))?.state, 'completed');
    assert.equal((await jobs(pool).get(other))?.state, 'completed');
    assert.equal((await jobs(pool).get(pending.id))?.state, 'pending');
    assert.equal((await jobs(pool).get(head.id))?.state, 'running');
    assert.equal((await jobs(pool).get(successor.id))?.blockedBy, head.id);
  } finally {
    stop.abort();
    await running;
  }
});

for (const retentionMs of [false, 30 * 86_400_000] as const) {
  test(`a retention override is applied to the selected definitions (${retentionMs})`, async () => {
    const def = definition();
    const recent = await terminal(def, 'completed', 10);
    const old = await terminal(def, 'completed', 40);
    const marker = await jobs(pool).send(def, 2);
    const stop = new AbortController();
    const running = runJobs(pool, [def], { signal: stop.signal, retentionMs });
    try {
      await until(async () => (await jobs(pool).get(marker.id))?.state === 'completed');
    } finally {
      stop.abort();
      await running;
    }
    assert.equal((await jobs(pool).get(recent))?.state, 'completed');
    assert.equal(
      (await jobs(pool).get(old))?.state ?? null,
      retentionMs === false ? 'completed' : null,
    );
  });
}

test('automatic retention is bounded, skips locks and coordinates competing cleanup passes', async () => {
  const def = definition();
  const rows = await jobs(pool).sendMany(
    def,
    Array.from({ length: 203 }, () => ({ payload: 1 })),
  );
  await pool.queryClassic(
    "UPDATE pgclear_jobs.jobs SET state = 'completed', finished_at = clock_timestamp() - interval '10 days' WHERE name = $1",
    [def.name],
  );
  await pool.transaction(async (tx) => {
    await tx.queryClassic('SELECT id FROM pgclear_jobs.jobs WHERE id = $1 FOR UPDATE', [
      rows[0]!.id,
    ]);
    const counts = await Promise.all([
      pruneCompleted(execute, [def.name], 7 * 86_400_000),
      pruneCompleted(execute, [def.name], 7 * 86_400_000),
    ]);
    assert.deepEqual(counts, [100, 100]);
    assert.equal(await pruneCompleted(execute, [def.name], 7 * 86_400_000), 2);
    assert.equal((await jobs(tx).get(rows[0]!.id))?.state, 'completed');
  });
  assert.equal(await pruneCompleted(execute, [def.name], 7 * 86_400_000), 1);
  assert.equal((await jobs(pool).stats(def)).total, 0);
});

test('automatic retention avoids scanning substantial retained failure history', async () => {
  const def = definition({ maxAttempts: 1 });
  const completed = await terminal(def, 'completed');
  const recent = await terminal(def, 'completed', 1);
  const policy = (await jobs(pool).get(completed))!.policy;
  await pool.queryClassic(
    `INSERT INTO pgclear_jobs.jobs (id, name, payload, policy, state, finished_at)
     SELECT md5($1 || n::text)::uuid, $1, '1'::jsonb, $2::jsonb, 'failed',
       clock_timestamp() - interval '30 days' FROM generate_series(1, 20000) AS n`,
    [def.name, JSON.stringify(policy)],
  );
  await pool.queryClassic('ANALYZE pgclear_jobs.jobs');
  // Inspect actual work, not elapsed time or a particular planner/index choice.
  interface Plan {
    readonly 'Rows Removed by Filter'?: number;
    readonly 'Actual Loops': number;
    readonly Plans?: readonly Plan[];
  }
  await pool.transaction(async (tx) => {
    await pruneCompleted(
      async (sql, values) => {
        const { rows } = await tx.queryClassic<{ 'QUERY PLAN': { Plan: Plan }[] }>(
          `EXPLAIN (ANALYZE, FORMAT JSON) ${sql}`,
          values,
        );
        const filtered = (plan: Plan): number =>
          (plan['Rows Removed by Filter'] ?? 0) * plan['Actual Loops'] +
          (plan.Plans ?? []).reduce((sum, child) => sum + filtered(child), 0);
        assert.ok(
          filtered(rows[0]!['QUERY PLAN'][0]!.Plan) < 100,
          'cleanup must not scan and filter the 20,000 retained failures',
        );
        return { rows: [] };
      },
      [def.name],
      7 * 86_400_000,
    );
    assert.equal(await jobs(tx).get(completed), null);
  });
  assert.equal(await pruneCompleted(execute, [def.name], 7 * 86_400_000), 0);
  assert.equal((await jobs(pool).get(recent))?.state, 'completed');
  assert.equal((await jobs(pool).stats(def)).failed, 20_000);
});

test('stable-ID reconciliation works within the automatic retention window and expires with history', async () => {
  const def = definition();
  const id = randomUUID();
  await jobs(pool).send(def, 1, { id });
  await finishClaim(execute, (await claimJob(execute, [def.name], 30_000)).claim!, null);
  assert.equal((await jobs(pool).send(def, 2, { id })).payload, 1);
  await pool.queryClassic(
    "UPDATE pgclear_jobs.jobs SET finished_at = clock_timestamp() - interval '10 days' WHERE id = $1",
    [id],
  );
  const stop = new AbortController();
  const running = runJobs(pool, [def], { signal: stop.signal });
  try {
    await until(async () => (await jobs(pool).get(id)) === null);
  } finally {
    stop.abort();
    await running;
  }
  const resubmitted = await jobs(pool).send(def, 2, { id });
  assert.equal(resubmitted.payload, 2);
  assert.equal(resubmitted.attempts, 0);
});

test('removed submission and schedule modes fail before SQL instead of silently adding independent jobs', async () => {
  const def = definition();
  await pool.transaction(async (tx) => {
    // @ts-expect-error Keyed work must explicitly use request.
    await assert.rejects(jobs(tx).send(def, 1, { key: 'entity' }), TypeError);
    // @ts-expect-error Independent batches do not coalesce.
    await assert.rejects(jobs(tx).sendMany(def, [{ payload: 1, key: 'entity' }]), TypeError);
    // @ts-expect-error Requests cannot use stable submission IDs.
    await assert.rejects(jobs(tx).request(def, 1, { key: 'entity', id: randomUUID() }), TypeError);
    await assert.rejects(
      // @ts-expect-error Schedules always catch up once.
      jobs(tx).schedule(def, 1, { cron: '* * * * *', missed: 'skip' }),
      TypeError,
    );
    await assert.rejects(
      // @ts-expect-error Schedules always prevent overlap.
      jobs(tx).schedule(def, 1, { cron: '* * * * *', overlap: 'allow' }),
      TypeError,
    );
    assert.equal((await jobs(tx).stats(def)).total, 0);
    await tx.queryClassic('SELECT 1');
  });
});
