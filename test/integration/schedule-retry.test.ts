import assert from 'node:assert/strict';
import test from 'node:test';
import { jobs, JobStateError } from '../../src/index.js';
import { claimJob, finishClaim } from '../../src/claims.js';
import { advanceSchedules } from '../../src/schedules.js';
import { definition, execute, pool, until } from '../helpers/database.js';

async function failedOccurrence() {
  const def = definition({ maxAttempts: 1 });
  const schedule = await jobs(pool).schedule(def, 1, { cron: '* * * * *' });
  await due(schedule.id, '2000-01-01');
  assert.equal(await advanceSchedules(pool, [def.name], 5000), 1);
  const original = (await jobs(pool).find(def))[0]!;
  await finishClaim(execute, (await claimJob(execute, [def.name], 30_000)).claim!, 'failed');
  await due(schedule.id, '2000-01-02');
  return { def, schedule, original };
}

async function due(id: string, at: string) {
  await pool.queryClassic('UPDATE pgclear_jobs.schedules SET next_run_at = $2 WHERE id = $1', [
    id,
    at,
  ]);
}

for (const state of ['pending', 'running'] as const) {
  test(`manual retry rejects an older failure while a later occurrence is ${state}`, async () => {
    const { def, schedule, original } = await failedOccurrence();
    assert.equal(await advanceSchedules(pool, [def.name], 5000), 1);
    const later = (await jobs(pool).find(def, { states: ['pending'] }))[0]!;
    const claim = state === 'running' ? (await claimJob(execute, [def.name], 30_000)).claim! : null;
    // A rejected retry must also leave a caller-owned transaction usable.
    await pool.transaction(async (tx) => {
      await assert.rejects(jobs(tx).retry(original.id), JobStateError);
      await tx.queryClassic('SELECT 1');
    });
    assert.equal((await jobs(pool).stats(def)).total, 2);
    const next = (await claimJob(execute, [def.name], 30_000)).claim;
    assert.equal(next?.id ?? null, state === 'pending' ? later.id : null);
    // Finish the live occurrence, then the original failure can be retried again.
    await finishClaim(execute, (claim ?? next)!, null);
    const retried = await jobs(pool).retry(original.id);
    assert.equal(retried.scheduleId, schedule.id);
    assert.deepEqual(retried.scheduledFor, original.scheduledFor);
    assert.equal(retried.retryOf, original.id);
  });
}

for (const winner of ['retry', 'scheduler'] as const) {
  test(`concurrent retry and scheduler serialize when ${winner} inserts first`, async () => {
    const { def, original } = await failedOccurrence();
    let pending: Promise<unknown> | undefined;
    let settled = false;
    try {
      await pool.transaction(async (tx) => {
        const { rows } = await tx.queryClassic<{ pid: number }>('SELECT pg_backend_pid() AS pid');
        if (winner === 'retry') {
          await jobs(tx).retry(original.id);
          pending = advanceSchedules(pool, [def.name], 5000);
        } else {
          assert.equal(
            await advanceSchedules(
              {
                queryClassic: pool.queryClassic.bind(pool),
                transaction: (work) => work(tx),
              },
              [def.name],
              5000,
            ),
            1,
          );
          pending = jobs(pool).retry(original.id);
        }
        // Observe both outcomes so a broken implementation cannot leave a rejected promise behind.
        void pending.then(
          () => {
            settled = true;
          },
          () => {
            settled = true;
          },
        );
        await until(async () => {
          const blocked = await pool.queryClassic<{ waiting: boolean }>(
            'SELECT EXISTS (SELECT FROM pg_stat_activity WHERE $1::integer = ANY(pg_blocking_pids(pid))) AS waiting',
            [rows[0]!.pid],
          );
          return settled || blocked.rows[0]!.waiting;
        });
        assert.equal(settled, false, 'the competing insert must wait for the outstanding job');
      });
      if (winner === 'retry') assert.equal(await pending, 0);
      else await assert.rejects(pending!, JobStateError);
      assert.equal((await jobs(pool).stats(def)).pending, 1);
      assert.ok((await claimJob(execute, [def.name], 30_000)).claim);
      assert.equal((await claimJob(execute, [def.name], 30_000)).claim, null);
    } finally {
      await pending?.catch(() => {});
    }
  });
}
