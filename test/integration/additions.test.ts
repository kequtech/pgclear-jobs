import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import test from 'node:test';
import { jobs, startJobs, JobStateError } from '../../src/index.js';
import { advanceSchedules } from '../../src/schedules.js';
import { claimJob, finishClaim } from '../../src/claims.js';
import { definition, execute, pool, until } from '../helpers/database.js';
import type { InputValue, QueryOptions, Transaction, TransactionOptions } from '@kequtech/pgclear';

test('bulk lookup preserves requested order, missing IDs, duplicates and transaction visibility', async (t) => {
  const a = await jobs(pool).send(definition(), 1);
  const b = await jobs(pool).send(definition(), 2);
  const statement = t.mock.method(pool, 'queryClassic', pool.queryClassic.bind(pool));
  assert.deepEqual(
    (await jobs(pool).getMany([b.id, randomUUID(), a.id, b.id.toUpperCase()])).map((j) => j.id),
    [b.id, a.id],
  );
  assert.equal(statement.mock.callCount(), 1);
  await assert.rejects(
    pool.transaction(async (tx) => {
      const hidden = await jobs(tx).send(definition(), 3);
      assert.equal((await jobs(tx).getMany([hidden.id])).length, 1);
      assert.deepEqual(await jobs(pool).getMany([hidden.id]), []);
      throw new Error('rollback');
    }),
    /rollback/,
  );
  assert.deepEqual(await jobs(pool).getMany([]), []);
  await assert.rejects(jobs(pool).getMany([a.id, 'bad']), TypeError);
  await assert.rejects(jobs(pool).getMany(Array<string>(1001).fill(a.id)), TypeError);
});

test('cursor projection reads beyond the default 100 records', async () => {
  const def = definition();
  await jobs(pool).sendMany(
    def,
    Array.from({ length: 101 }, (_, payload) => ({ payload })),
  );
  const ids: string[] = [];
  let after: string | undefined;
  for (;;) {
    const page = await jobs(pool).find(def, { ...(after ? { after } : {}) });
    if (!page.length) break;
    ids.push(...page.map((job) => job.id));
    after = page.at(-1)!.cursor;
  }
  assert.equal(new Set(ids).size, 101);
  assert.equal((await jobs(pool).getMany(ids)).length, 101);
});

test('the handle awaits startup and preserves supervision and pool ownership', async () => {
  const stop = new AbortController();
  const release = Promise.withResolvers<void>();
  const entered = Promise.withResolvers<void>();
  const def = definition();
  const job = await jobs(pool).send(def, 1);
  const handle = startJobs(pool, [def], {
    signal: stop.signal,
    pollIntervalMs: 10,
    onReady() {
      entered.resolve();
      return release.promise;
    },
  });
  try {
    await entered.promise;
    assert.equal((await jobs(pool).get(job.id))!.state, 'pending');
    release.resolve();
    await handle.ready;
    await until(async () => (await jobs(pool).get(job.id))!.state === 'completed');
  } finally {
    release.resolve();
    stop.abort();
    await handle.done;
  }
  assert.equal((await pool.queryClassic('SELECT 1')).rows.length, 1);
});

test('keyed schedules and manual requests share a head and one successor across runners', async () => {
  const def = definition({ maxAttempts: 2, retryDelayMs: 60000 });
  const queue = jobs(pool);
  const head = await queue.request(def, 0, { key: 'maintenance' });
  const claim = (await claimJob(execute, [def.name], 30000)).claim!;
  await finishClaim(execute, claim, 'retry');
  const retry = (await queue.get(head.id))!;
  const first = await queue.schedule(def, 1, {
    key: 'hourly',
    requestKey: 'maintenance',
    cron: '* * * * *',
  });
  const second = await queue.schedule(def, 2, {
    key: 'daily',
    requestKey: 'maintenance',
    cron: '0 0 * * *',
  });
  await pool.queryClassic(
    "UPDATE pgclear_jobs.schedules SET next_run_at = '2000-01-01' WHERE name = $1",
    [def.name],
  );
  await Promise.all([
    advanceSchedules(pool, [def.name], 5000),
    advanceSchedules(pool, [def.name], 5000),
  ]);
  const outstanding = await queue.find(def);
  assert.equal(outstanding.length, 2);
  const successor = outstanding.find((j) => j.blockedBy === head.id)!;
  assert.ok(successor);
  assert.equal(successor.scheduleId, null);
  assert.equal(successor.scheduledFor, null);
  assert.equal((await queue.get(head.id))!.runAt.getTime(), retry.runAt.getTime());
  assert.ok(
    (await queue.schedules(def)).every(
      (s) => s.lastJobId === successor.id && s.nextRunAt > new Date(),
    ),
  );
  await queue.reschedule(successor.id, new Date('2030-01-01'));
  await pool.queryClassic(
    "UPDATE pgclear_jobs.schedules SET next_run_at = '2000-01-01' WHERE id = $1",
    [first.id],
  );
  await advanceSchedules(pool, [def.name], 5000);
  assert.equal((await queue.get(successor.id))!.runAt.toISOString(), '2030-01-01T00:00:00.000Z');
  assert.equal((await queue.request(def, 3, { key: 'maintenance' })).id, successor.id);
  await queue.unschedule(def, 'hourly');
  await queue.unschedule(def, 'daily');
  assert.ok(await queue.get(successor.id));
  await queue.reschedule(head.id, new Date(0));
  await finishClaim(execute, (await claimJob(execute, [def.name], 30000)).claim!, 'failed');
  await assert.rejects(queue.retry(head.id), JobStateError);
  await queue.cancel(successor.id);
  const retried = await queue.retry(head.id);
  assert.equal(retried.key, 'maintenance');
  assert.equal(retried.scheduleId, null);
  assert.equal(second.requestKey, 'maintenance');
});

test('schedule edits preserve due time and advancing requests rolls back with the cursor', async () => {
  const def = definition();
  const first = await jobs(pool).schedule(def, 1, { cron: '* * * * *' });
  await pool.queryClassic(
    "UPDATE pgclear_jobs.schedules SET next_run_at = '2000-01-01' WHERE id = $1",
    [first.id],
  );
  const edited = await jobs(pool).schedule(def, 2, { cron: '* * * * *', requestKey: 'shared' });
  assert.equal(edited.nextRunAt.toISOString(), '2000-01-01T00:00:00.000Z');
  await assert.rejects(
    pool.transaction(async (tx) => {
      await advanceSchedules(
        { queryClassic: tx.queryClassic.bind(tx), transaction: (work) => work(tx) },
        [def.name],
        5000,
      );
      assert.equal((await jobs(tx).find(def)).length, 1);
      throw new Error('rollback');
    }),
    /rollback/,
  );
  assert.deepEqual(await jobs(pool).find(def), []);
  assert.equal((await jobs(pool).schedules(def))[0]!.lastJobId, null);
  await Promise.all([
    advanceSchedules(pool, [def.name], 5000),
    jobs(pool).request(def, 3, { key: 'shared' }),
  ]);
  assert.equal((await jobs(pool).find(def)).length, 1);
  const ordinary = await jobs(pool).schedule(def, 4, { cron: '* * * * *' });
  assert.equal(ordinary.requestKey, null);
});

test('parallel schedule batches acquire shared request keys in the same order', async () => {
  const def = definition();
  const queue = jobs(pool);
  const heads = new Map([
    ['a', (await queue.request(def, 0, { key: 'a' })).id],
    ['b', (await queue.request(def, 0, { key: 'b' })).id],
  ]);
  for (let index = 0; index < 100; index++) {
    await queue.schedule(def, index, {
      cron: '* * * * *',
      key: `slot:${index}`,
      requestKey: index < 25 || index >= 75 ? 'a' : 'b',
    });
  }
  await pool.queryClassic(
    `UPDATE pgclear_jobs.schedules SET next_run_at =
    '2000-01-01'::timestamptz + split_part(key, ':', 2)::integer * interval '1 millisecond'
    WHERE name=$1`,
    [def.name],
  );
  const selected = Promise.withResolvers<void>();
  let batches = 0;
  const concurrent = new Proxy(pool, {
    get(target, key) {
      if (key === 'transaction')
        return <T>(work: (tx: Transaction) => Promise<T>, options?: TransactionOptions) =>
          target.transaction(
            (tx) =>
              work(
                new Proxy(tx, {
                  get(scope, member) {
                    if (member === 'queryClassic')
                      return async <Row extends object>(
                        text: string,
                        values?: readonly InputValue[],
                        settings?: QueryOptions,
                      ) => {
                        const result = await scope.queryClassic<Row>(text, values, settings);
                        const first = result.rows[0] as
                          { record?: { request_key?: unknown } } | undefined;
                        if (first?.record && 'request_key' in first.record) {
                          if (++batches === 2) selected.resolve();
                          await selected.promise;
                        }
                        return result;
                      };
                    const value: unknown = Reflect.get(scope, member, scope);
                    return typeof value === 'function' ? value.bind(scope) : value;
                  },
                }),
              ),
            options,
          );
      const value: unknown = Reflect.get(target, key, target);
      return typeof value === 'function' ? value.bind(target) : value;
    },
  });
  const results = await Promise.allSettled([
    advanceSchedules(concurrent, [def.name], 10000),
    advanceSchedules(concurrent, [def.name], 10000),
  ]);
  for (const result of results) if (result.status === 'rejected') throw result.reason;
  assert.equal(batches, 2);
  assert.equal((await queue.find(def)).length, 2);
  const schedules = await queue.schedules(def, { limit: 1000 });
  assert.equal(schedules.length, 100);
  assert.ok(
    schedules.every((s) => s.nextRunAt > new Date() && s.lastJobId === heads.get(s.requestKey!)),
  );
});
