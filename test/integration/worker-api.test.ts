import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { setImmediate as nextTurn } from 'node:timers/promises';
import test from 'node:test';
import type { TestContext } from 'node:test';
import { defineJob, defineWorker, jobs, JobError, runJobs } from '../../src/index.js';
import type { JobDefinition, RunnerEvent, RunnerOptions } from '../../src/index.js';
import type { Transaction, TransactionOptions } from '@kequtech/pgclear';
import { definition, pool, until } from '../helpers/database.js';

function start(worker: JobDefinition<unknown>, options: RunnerOptions = {}) {
  const stop = new AbortController();
  const running = runJobs(pool, [worker], {
    signal: stop.signal,
    pollIntervalMs: 10,
    recoveryIntervalMs: 30_000,
    shutdownTimeoutMs: 0,
    ...options,
  });
  void running.catch(() => {});
  return { stop, running };
}
async function appTable(t: TestContext) {
  const table = `public.worker_api_${randomUUID().replaceAll('-', '')}`;
  await pool.queryClassic(`CREATE TABLE ${table} (value integer NOT NULL)`);
  t.after(() => pool.queryClassic(`DROP TABLE ${table}`));
  return table;
}

test('handler-free producer submits and registers schedules for a separately bound worker', async () => {
  const spec = defineJob<{ value: number }>({ name: definition().name, maxAttempts: 2 });
  const worker = defineWorker(spec, {
    handler(payload) {
      assert.equal(typeof payload.value, 'number');
    },
  });
  const sent = await jobs(pool).send(spec, { value: 1 });
  await jobs(pool).request(spec, { value: 2 }, { key: 'shared' });
  await jobs(pool).sendMany(spec, [{ payload: { value: 3 } }]);
  const schedule = await jobs(pool).schedule(spec, { value: 4 }, { cron: '* * * * *' });
  assert.equal(schedule.policy.maxAttempts, 2);
  assert.equal((await jobs(pool).find(spec)).length, 3);
  const { stop, running } = start(worker);
  try {
    await until(async () => (await jobs(pool).stats(spec)).completed === 3);
  } finally {
    stop.abort();
    await running;
  }
  assert.equal((await jobs(pool).get(sent.id))?.policy.maxAttempts, 2);
});

test('events report committed retry timestamps and original handler errors without payload fields', async () => {
  const error = { transient: true };
  const spec = defineJob<{ secret: string }>({
    name: definition().name,
    retryDelayMs: 100,
    retryJitter: true,
    maxAttempts: 2,
  });
  const worker = defineWorker(spec, {
    handler(_payload, { attempt }) {
      if (attempt === 1) throw error;
    },
  });
  const events: RunnerEvent[] = [];
  const job = await jobs(pool).send(spec, { secret: 'not-an-event-field' });
  const { stop, running } = start(worker, {
    onEvent(event) {
      events.push(event);
    },
    onHandlerError(failure) {
      assert.equal(failure.error, error);
      assert.equal(failure.transactional, false);
      return 'continue';
    },
  });
  try {
    await until(async () => events.some((e) => e.type === 'attemptCompleted'));
  } finally {
    stop.abort();
    await running;
  }
  assert.deepEqual(
    events.map((e) => e.type),
    ['attemptStarted', 'attemptFailed', 'attemptStarted', 'attemptCompleted'],
  );
  const failure = events.find((e) => e.type === 'attemptFailed')!;
  assert.equal(failure.error, error);
  assert.equal(failure.retryAt?.getTime(), (await jobs(pool).get(job.id))?.runAt.getTime());
  for (const event of events) assert.equal('payload' in event, false);
});

for (const error of [
  undefined,
  null,
  { original: true },
  new JobError('permanent', { retry: false }),
]) {
  test(`error policy preserves a thrown ${error === undefined ? 'undefined' : error === null ? 'null' : error instanceof JobError ? 'JobError' : 'object'}`, async () => {
    const spec = defineJob<number>({ name: definition().name, maxAttempts: 1 });
    const worker = defineWorker(spec, {
      handler() {
        throw error;
      },
    });
    const events: RunnerEvent[] = [];
    const job = await jobs(pool).send(spec, 1);
    const { stop, running } = start(worker, {
      onEvent(event) {
        events.push(event);
      },
      onHandlerError(failure) {
        assert.equal(failure.error, error);
        return 'continue';
      },
    });
    try {
      await until(async () => events.some((e) => e.type === 'attemptFailed'));
    } finally {
      stop.abort();
      await running;
    }
    const failure = events.find((e) => e.type === 'attemptFailed')!;
    assert.equal(failure.error, error);
    assert.equal(failure.retryAt, null);
    assert.equal((await jobs(pool).get(job.id))?.state, 'failed');
  });
}

for (const transactional of [false, true] as const) {
  test(`${transactional ? 'managed' : 'ordinary'} error policy stops with the original value before bookkeeping`, async (t) => {
    const table = await appTable(t);
    const error = { infrastructure: true };
    const spec = defineJob<number>({ name: definition().name });
    const worker = transactional
      ? defineWorker(spec, {
          transactional: true,
          async handler(_value, { tx }) {
            await tx.queryClassic(`INSERT INTO ${table} VALUES (1)`);
            throw error;
          },
        })
      : defineWorker(spec, {
          handler() {
            throw error;
          },
        });
    const events: RunnerEvent[] = [];
    const job = await jobs(pool).send(spec, 1);
    let policyCalls = 0;
    const { running } = start(worker, {
      onEvent(event) {
        events.push(event);
      },
      async onHandlerError(failure): Promise<'stop'> {
        policyCalls++;
        assert.equal(failure.error, error);
        assert.equal(failure.transactional, transactional);
        assert.deepEqual((await pool.queryClassic(`SELECT * FROM ${table}`)).rows, []);
        assert.equal((await jobs(pool).get(job.id))?.state, 'running');
        assert.equal((await jobs(pool).attempts(job.id))[0]?.outcome, null);
        return 'stop';
      },
    });
    await assert.rejects(running, (e) => e === error);
    assert.equal(policyCalls, 1);
    assert.equal((await jobs(pool).get(job.id))?.state, 'running');
    assert.deepEqual(
      events.map((e) => e.type),
      ['attemptStarted', 'runnerFailed'],
    );
    const failed = events.find((e) => e.type === 'runnerFailed')!;
    assert.equal(failed.error, error);
    assert.equal(failed.attempt?.id, job.id);
  });
}

for (const invalid of [false, true]) {
  test(`${invalid ? 'invalid' : 'rejected'} error policy preserves both errors without recording a normal failure`, async () => {
    const original = new Error('handler failed');
    const policy = new Error('policy failed');
    const spec = defineJob<number>({ name: definition().name });
    const worker = defineWorker(spec, {
      handler() {
        throw original;
      },
    });
    const job = await jobs(pool).send(spec, 1);
    const { running } = start(worker, {
      onHandlerError: (() => {
        if (invalid) return 'retry';
        return Promise.reject(policy);
      }) as never,
    });
    await assert.rejects(running, (error) => {
      assert.ok(error instanceof AggregateError);
      assert.equal(error.errors[0], original);
      assert.ok(invalid ? error.errors[1] instanceof TypeError : error.errors[1] === policy);
      return true;
    });
    assert.equal((await jobs(pool).attempts(job.id))[0]?.outcome, null);
  });
}

test('a pending error policy is cancelled and a late rejection is observed after managed rollback', async (t) => {
  const table = await appTable(t);
  const entered = Promise.withResolvers<void>();
  const release = Promise.withResolvers<'continue'>();
  const spec = defineJob<number>({ name: definition().name });
  const worker = defineWorker(spec, {
    transactional: true,
    async handler(_value, { tx }) {
      await tx.queryClassic(`INSERT INTO ${table} VALUES (1)`);
      throw new Error('handler failed');
    },
  });
  const events: RunnerEvent[] = [];
  const job = await jobs(pool).send(spec, 1);
  const { stop, running } = start(worker, {
    onEvent(event) {
      events.push(event);
    },
    onHandlerError() {
      entered.resolve();
      return release.promise;
    },
  });
  try {
    await entered.promise;
    stop.abort();
    await running;
    assert.deepEqual((await pool.queryClassic(`SELECT * FROM ${table}`)).rows, []);
    assert.equal((await jobs(pool).attempts(job.id))[0]?.outcome, null);
    assert.deepEqual(
      events.map((e) => e.type),
      ['attemptStarted', 'attemptAbandoned'],
    );
    const event = events.find((e) => e.type === 'attemptAbandoned')!;
    assert.equal(event.reason, 'runnerStopped');
    release.reject(new Error('late policy error'));
    await nextTurn();
  } finally {
    release.resolve('continue');
    stop.abort();
    await running;
  }
});

test('pending/failed async observers do not block completion or shutdown', async (t) => {
  const warnings: unknown[] = [];
  t.mock.method(process, 'emitWarning', (...args: unknown[]) => {
    warnings.push(args);
  });
  const release = Promise.withResolvers<void>();
  const spec = defineJob<number>({ name: definition().name });
  const worker = defineWorker(spec, { handler() {} });
  const job = await jobs(pool).send(spec, 1);
  const { stop, running } = start(worker, {
    onEvent() {
      return release.promise;
    },
  });
  try {
    await until(async () => (await jobs(pool).get(job.id))?.state === 'completed');
  } finally {
    stop.abort();
    await running;
  }
  release.reject(new Error('logger failed'));
  await nextTurn();
  assert.equal((await jobs(pool).get(job.id))?.state, 'completed');
  assert.equal(warnings.length, 2);
});

test('confirmed managed completion is observed after cancellation wins the earlier race', async () => {
  const acknowledged = Promise.withResolvers<void>();
  const release = Promise.withResolvers<void>();
  const spec = defineJob<number>({ name: definition().name });
  const worker = defineWorker(spec, { transactional: true, handler() {} });
  const events: RunnerEvent[] = [];
  const client = new Proxy(pool, {
    get(target, key) {
      if (key === 'transaction')
        return async <T>(work: (tx: Transaction) => Promise<T>, options?: TransactionOptions) => {
          const result = await pool.transaction(work, options);
          acknowledged.resolve();
          await release.promise;
          return result;
        };
      const value: unknown = Reflect.get(target, key, target);
      return typeof value === 'function' ? value.bind(target) : value;
    },
  });
  const stop = new AbortController();
  const job = await jobs(pool).send(spec, 1);
  const running = runJobs(client, [worker], {
    signal: stop.signal,
    shutdownTimeoutMs: 0,
    recoveryIntervalMs: 30_000,
    onEvent(event) {
      events.push(event);
    },
  });
  void running.catch(() => {});
  try {
    await acknowledged.promise;
    assert.equal((await jobs(pool).get(job.id))?.state, 'completed');
    assert.deepEqual(
      events.map((e) => e.type),
      ['attemptStarted'],
    );
    stop.abort();
    release.resolve();
    await running;
    assert.deepEqual(
      events.map((e) => e.type),
      ['attemptStarted', 'attemptCompleted'],
    );
  } finally {
    stop.abort();
    release.resolve();
    await running;
  }
});

test('runner failure aggregates another active transaction cleanup failure', async () => {
  const entered = Promise.withResolvers<void>();
  const release = Promise.withResolvers<void>();
  const primary = new Error('stop the runner');
  const cleanup = new Error('secondary cleanup failed');
  let cleanupAggregate: AggregateError | undefined;
  const a = defineJob<number>({ name: definition().name });
  const b = defineJob<number>({ name: definition().name });
  const first = defineWorker(a, {
    transactional: true,
    async handler() {
      await entered.promise;
      throw primary;
    },
  });
  const second = defineWorker(b, {
    transactional: true,
    async handler() {
      entered.resolve();
      await release.promise;
    },
  });
  let transactions = 0;
  const client = new Proxy(pool, {
    get(target, key) {
      if (key === 'transaction')
        return async <T>(work: (tx: Transaction) => Promise<T>, options?: TransactionOptions) => {
          const index = ++transactions;
          try {
            return await pool.transaction(work, options);
          } catch (error) {
            if (index !== 2) throw error;
            cleanupAggregate = new AggregateError([error, cleanup], 'Injected cleanup failure');
            throw cleanupAggregate;
          }
        };
      const value: unknown = Reflect.get(target, key, target);
      return typeof value === 'function' ? value.bind(target) : value;
    },
  });
  await jobs(pool).send(a, 1);
  await jobs(pool).send(b, 1);
  const events: RunnerEvent[] = [];
  const stop = new AbortController();
  const running = runJobs(client, [first, second], {
    signal: stop.signal,
    concurrency: 2,
    recoveryIntervalMs: 30_000,
    shutdownTimeoutMs: 0,
    onHandlerError: () => 'stop',
    onEvent(event) {
      events.push(event);
    },
  });
  try {
    await assert.rejects(running, (error) => {
      assert.ok(error instanceof AggregateError);
      assert.equal(error.errors[0], primary);
      assert.equal(error.errors[1], cleanupAggregate);
      assert.equal(cleanupAggregate?.errors[1], cleanup);
      return true;
    });
    const failed = events.filter((e) => e.type === 'runnerFailed');
    assert.equal(failed.length, 1);
    assert.ok(failed[0]!.error instanceof AggregateError);
  } finally {
    stop.abort();
    release.resolve();
    await running.catch(() => {});
  }
});

for (const reason of ['deadlineExceeded', 'leaseLost'] as const) {
  test(`local ${reason} produces abandonment without a false outcome`, async () => {
    const entered = Promise.withResolvers<void>();
    const spec = defineJob<number>({
      name: definition().name,
      maxAttempts: 1,
      executionTimeoutMs: reason === 'deadlineExceeded' ? 250 : 30_000,
    });
    const worker = defineWorker(spec, {
      async handler(_payload, { signal }) {
        entered.resolve();
        await new Promise<void>((resolve) =>
          signal.addEventListener('abort', () => resolve(), { once: true }),
        );
      },
    });
    const events: RunnerEvent[] = [];
    const job = await jobs(pool).send(spec, 1);
    const { stop, running } = start(worker, {
      leaseMs: reason === 'deadlineExceeded' ? 30_000 : 600,
      onEvent(event) {
        events.push(event);
      },
      onHandlerError() {
        throw new Error('Cancellation must not enter handler error policy');
      },
    });
    try {
      await entered.promise;
      if (reason === 'leaseLost')
        await pool.queryClassic('UPDATE pgclear_jobs.jobs SET claim_token = $2 WHERE id = $1', [
          job.id,
          randomUUID(),
        ]);
      await until(async () => events.some((e) => e.type === 'attemptAbandoned'));
      assert.deepEqual(
        events.map((e) => e.type),
        ['attemptStarted', 'attemptAbandoned'],
      );
      assert.equal(events.find((e) => e.type === 'attemptAbandoned')!.reason, reason);
    } finally {
      stop.abort();
      await running;
    }
  });
}
