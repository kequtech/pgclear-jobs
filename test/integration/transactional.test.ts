import assert from 'node:assert/strict';
import test from 'node:test';
import type { TestContext } from 'node:test';
import { createPool, DatabaseError, DriverError } from '@kequtech/pgclear';
import type { Pool, Transaction, TransactionOptions } from '@kequtech/pgclear';
import { defineJob, jobs, JobError, runJobs } from '../../src/index.js';
import type { HandlerContext, JobDefinition, JobRecords } from '../../src/index.js';
import type { RunnerOptions } from '../../src/index.js';
import { createJobsIntegration } from '../../src/integration.js';
import { claimJob, finishClaim, recoverClaims } from '../../src/claims.js';
import type { Execute } from '../../src/storage.js';
import { definition, execute, pool, until, url } from '../helpers/database.js';
import { connectionProxy } from '../helpers/connection-proxy.js';

type Adapter = 'pgclear' | 'integration';
interface Scope {
  readonly execute: Execute;
}
interface Database extends Scope {
  readonly pool: Pool;
}
const integrated = createJobsIntegration<Database, Scope>({
  pool: (database) => database.pool,
  execute: (scope) => scope.execute,
  transaction: (database, work, options) =>
    database.pool.transaction(
      (tx) => work({ execute: (sql, values) => tx.queryClassic(sql, values) }),
      options,
    ),
});
const runners = new Map<string, (client: Pool, options: RunnerOptions) => Promise<void>>();
interface Context extends HandlerContext {
  readonly query: Execute;
  readonly records: JobRecords;
}

function transactional(
  adapter: Adapter,
  work: (payload: number, context: Context) => Promise<void>,
  options: { maxAttempts?: number; executionTimeoutMs?: number } = {},
) {
  const settings = {
    name: definition().name,
    retryDelayMs: 0,
    retryJitter: false,
    transactional: true as const,
    ...options,
  };
  if (adapter === 'pgclear') {
    const def = defineJob({
      ...settings,
      handler: (payload: number, context) =>
        work(payload, {
          ...context,
          query: (sql, values) => context.tx.queryClassic(sql, values),
          records: jobs(context.tx),
        }),
    });
    runners.set(def.name, (client, options) => runJobs(client, [def], options));
    return def;
  }
  const def = integrated.defineJob({
    ...settings,
    handler: (payload: number, context) =>
      work(payload, {
        ...context,
        query: context.tx.execute,
        records: integrated.jobs(context.tx),
      }),
  });
  runners.set(def.name, (client, options) =>
    integrated.runJobs(
      { pool: client, execute: (sql, values) => client.queryClassic(sql, values) },
      [def],
      options,
    ),
  );
  return def;
}

async function table(t: TestContext) {
  const name = `public.${definition().name.replaceAll('-', '_')}`;
  await pool.queryClassic(`CREATE TABLE ${name} (value integer NOT NULL)`);
  t.after(() => pool.queryClassic(`DROP TABLE ${name}`));
  return name;
}

function start(def: JobDefinition<number, never>, options: Parameters<typeof runJobs>[2] = {}) {
  const stop = new AbortController();
  const running = runners.get(def.name)!(pool, {
    signal: stop.signal,
    pollIntervalMs: 10,
    recoveryIntervalMs: 20,
    shutdownTimeoutMs: 20,
    ...options,
  });
  void running.catch(() => {});
  return { stop, running };
}

for (const adapter of ['pgclear', 'integration'] as const) {
  test(`${adapter} handler writes, follow-up submission and completion commit together`, async (t) => {
    const app = await table(t);
    const child = definition();
    const release = Promise.withResolvers<void>();
    let context: Context | undefined;
    let childId: string | undefined;
    const def = transactional(adapter, async (value, scope) => {
      context = scope;
      await scope.query(`INSERT INTO ${app} VALUES ($1)`, [value]);
      childId = (await scope.records.send(child, value)).id;
      await release.promise;
    });
    const job = await jobs(pool).send(def, 7);
    const { stop, running } = start(def);
    try {
      await until(async () => childId !== undefined);
      assert.equal((await pool.queryClassic(`SELECT * FROM ${app}`)).rows.length, 0);
      assert.equal(await jobs(pool).get(childId!), null);
      assert.equal((await jobs(pool).get(job.id))?.state, 'running');
      release.resolve();
      await until(async () => (await jobs(pool).get(job.id))?.state === 'completed');
      assert.deepEqual((await pool.queryClassic(`SELECT * FROM ${app}`)).rows, [{ value: 7 }]);
      assert.equal((await jobs(pool).get(childId!))?.state, 'pending');
      assert.equal((await jobs(pool).attempts(job.id))[0]?.outcome, 'completed');
      await assert.rejects(context!.query('SELECT 1'));
    } finally {
      release.resolve();
      stop.abort();
      await running;
    }
    assert.equal((await pool.queryClassic('SELECT 1')).rows.length, 1);
  });

  test(`${adapter} rolls back handler writes and follow-ups before retrying with a fresh transaction`, async (t) => {
    const app = await table(t);
    const child = definition();
    const def = transactional(
      adapter,
      async (_value, { query, records, attempt }) => {
        assert.equal((await query(`SELECT * FROM ${app}`)).rows.length, 0);
        assert.equal((await records.find(child)).length, 0);
        await query(`INSERT INTO ${app} VALUES ($1)`, [attempt]);
        await records.send(child, attempt);
        if (attempt === 1) throw new Error('retry me');
      },
      { maxAttempts: 2 },
    );
    const job = await jobs(pool).send(def, 1);
    const { stop, running } = start(def);
    try {
      await until(async () => (await jobs(pool).get(job.id))?.state === 'completed');
      assert.deepEqual((await pool.queryClassic(`SELECT * FROM ${app}`)).rows, [{ value: 2 }]);
      assert.deepEqual(
        (await jobs(pool).find(child)).map((job) => job.payload),
        [2],
      );
      assert.deepEqual(
        (await jobs(pool).attempts(job.id)).map((a) => a.outcome),
        ['completed', 'failed'],
      );
    } finally {
      stop.abort();
      await running;
    }
  });

  test(`${adapter} preserves permanent failure diagnostics after rolling back application writes`, async (t) => {
    const app = await table(t);
    const def = transactional(adapter, async (_value, { query }) => {
      await query(`INSERT INTO ${app} VALUES (1)`);
      throw new JobError('permanent failure', { retry: false });
    });
    const job = await jobs(pool).send(def, 1);
    const { stop, running } = start(def);
    try {
      await until(async () => (await jobs(pool).get(job.id))?.state === 'failed');
      assert.equal((await pool.queryClassic(`SELECT * FROM ${app}`)).rows.length, 0);
      assert.equal((await jobs(pool).attempts(job.id)).length, 1);
      assert.match((await jobs(pool).get(job.id))!.lastError!, /permanent failure/);
    } finally {
      stop.abort();
      await running;
    }
  });

  test(`${adapter} SQL errors roll back earlier writes and record a failed attempt`, async (t) => {
    const app = await table(t);
    const def = transactional(
      adapter,
      async (_value, { query }) => {
        await query(`INSERT INTO ${app} VALUES (1)`);
        await query(`INSERT INTO ${app} VALUES (NULL)`);
      },
      { maxAttempts: 1 },
    );
    const job = await jobs(pool).send(def, 1);
    const { stop, running } = start(def);
    try {
      await until(async () => (await jobs(pool).get(job.id))?.state === 'failed');
      assert.equal((await pool.queryClassic(`SELECT * FROM ${app}`)).rows.length, 0);
      const attempts = await jobs(pool).attempts(job.id);
      assert.equal(attempts.length, 1);
      assert.equal(attempts[0]!.outcome, 'failed');
      assert.match(attempts[0]!.error!, /not-null constraint/);
    } finally {
      stop.abort();
      await running;
    }
  });

  test(`${adapter} refuses completion and rolls back writes after another attempt acquires ownership`, async (t) => {
    const app = await table(t);
    await pool.queryClassic(`INSERT INTO ${app} VALUES (0)`);
    const release = Promise.withResolvers<void>();
    let written = false;
    const def = transactional(adapter, async (_value, { query }) => {
      await query(`UPDATE ${app} SET value = 1`);
      written = true;
      await release.promise;
    });
    const job = await jobs(pool).send(def, 1);
    const { stop, running } = start(def, { recoveryIntervalMs: 30_000 });
    try {
      await until(async () => written);
      await pool.queryClassic(
        "UPDATE pgclear_jobs.jobs SET lease_until = clock_timestamp() - interval '1 second' WHERE id = $1",
        [job.id],
      );
      assert.equal(await recoverClaims(execute), 1);
      const next = (await claimJob(execute, [def.name], 30_000)).claim!;
      assert.equal(next.attempt_count, 2);
      release.resolve();
      // An UPDATE must obtain the handler's row lock: this proves rollback actually finished.
      assert.deepEqual(
        (
          await pool.queryClassic(`UPDATE ${app} SET value = value RETURNING value`, [], {
            timeoutMs: 2000,
          })
        ).rows,
        [{ value: 0 }],
      );
      assert.equal((await jobs(pool).get(job.id))?.state, 'running');
      assert.equal(await finishClaim(execute, next, null), true);
    } finally {
      release.resolve();
      stop.abort();
      await running;
    }
  });

  test(`${adapter} renews ownership while application writes remain uncommitted`, async (t) => {
    const app = await table(t);
    const release = Promise.withResolvers<void>();
    let written = false;
    const def = transactional(adapter, async (_value, { query }) => {
      await query(`INSERT INTO ${app} VALUES (1)`);
      written = true;
      await release.promise;
    });
    const job = await jobs(pool).send(def, 1);
    // Allow clock/scheduling jitter while still holding the handler beyond its original lease.
    const leaseMs = 3000;
    const { stop, running } = start(def, { leaseMs });
    try {
      await until(async () => written);
      const initial = (
        await pool.queryClassic<{ lease: string }>(
          'SELECT to_jsonb(lease_until) AS lease FROM pgclear_jobs.jobs WHERE id = $1',
          [job.id],
        )
      ).rows[0]!.lease;
      await until(async () => {
        const { rows } = await pool.queryClassic<{
          state: string;
          attempt_count: number;
          renewed: boolean;
          last_error: string | null;
          lease_until: string | null;
          observed_at: string;
        }>(
          "SELECT state, attempt_count, last_error, to_jsonb(lease_until) AS lease_until, to_jsonb(clock_timestamp()) AS observed_at, lease_until > $2::timestamptz + $3::double precision * interval '1 millisecond' AS renewed FROM pgclear_jobs.jobs WHERE id = $1",
          [job.id, initial, leaseMs],
        );
        assert.equal(rows[0]!.state, 'running', JSON.stringify({ initial, ...rows[0] }));
        assert.equal(rows[0]!.attempt_count, 1);
        return rows[0]!.renewed;
      });
      assert.equal((await jobs(pool).get(job.id))?.attempts, 1);
      release.resolve();
      await until(async () => (await jobs(pool).get(job.id))?.state === 'completed');
    } finally {
      release.resolve();
      stop.abort();
      await running;
    }
  });

  test(`${adapter} rolls back when a completion lock wait outlives its database lease`, async (t) => {
    const app = await table(t);
    await pool.queryClassic(`INSERT INTO ${app} VALUES (0)`);
    const release = Promise.withResolvers<void>();
    let pid: number | undefined;
    const def = transactional(
      adapter,
      async (_value, { query }) => {
        await query(`UPDATE ${app} SET value = 1`);
        pid = (await query<{ pid: number }>('SELECT pg_backend_pid() AS pid')).rows[0]!.pid;
        await release.promise;
      },
      { maxAttempts: 1 },
    );
    const job = await jobs(pool).send(def, 1);
    const { stop, running } = start(def, { recoveryIntervalMs: 30_000 });
    try {
      await until(async () => pid !== undefined);
      await pool.transaction(async (tx) => {
        await tx.queryClassic('SELECT FROM pgclear_jobs.jobs WHERE id = $1 FOR UPDATE', [job.id]);
        release.resolve();
        await until(
          async () =>
            (
              await pool.queryClassic<{ blocked: boolean }>(
                'SELECT cardinality(pg_blocking_pids($1)) > 0 AS blocked',
                [pid!],
              )
            ).rows[0]!.blocked,
        );
        await tx.queryClassic(
          "UPDATE pgclear_jobs.jobs SET lease_until = clock_timestamp() - interval '1 second' WHERE id = $1",
          [job.id],
        );
      });
      // Requiring the application row lock proves that the stale transaction rolled back.
      assert.deepEqual(
        (
          await pool.queryClassic(`UPDATE ${app} SET value = value RETURNING value`, [], {
            timeoutMs: 2000,
          })
        ).rows,
        [{ value: 0 }],
      );
      assert.equal((await jobs(pool).get(job.id))?.state, 'running');
      assert.equal(await recoverClaims(execute), 1);
      assert.equal((await jobs(pool).get(job.id))?.state, 'failed');
    } finally {
      release.resolve();
      stop.abort();
      await running;
    }
  });

  test(`${adapter} bounds a blocked completion through the transaction signal`, async (t) => {
    const app = await table(t);
    await pool.queryClassic(`INSERT INTO ${app} VALUES (0)`);
    const release = Promise.withResolvers<void>();
    let written = false;
    const def = transactional(adapter, async (_value, { query }) => {
      await query(`UPDATE ${app} SET value = 1`);
      written = true;
      await release.promise;
    });
    const job = await jobs(pool).send(def, 1);
    const { stop, running } = start(def, { databaseTimeoutMs: 100, recoveryIntervalMs: 30_000 });
    try {
      await until(async () => written);
      await pool.transaction(async (tx) => {
        await tx.queryClassic('SELECT FROM pgclear_jobs.jobs WHERE id = $1 FOR UPDATE', [job.id]);
        release.resolve();
        await assert.rejects(
          running,
          (error: unknown) => error instanceof DriverError && error.code === 'PGCLEAR_TIMEOUT',
        );
        assert.deepEqual(
          (
            await pool.queryClassic(`UPDATE ${app} SET value = value RETURNING value`, [], {
              timeoutMs: 2000,
            })
          ).rows,
          [{ value: 0 }],
        );
      });
      assert.equal((await jobs(pool).get(job.id))?.state, 'running');
    } finally {
      release.resolve();
      stop.abort();
      await running.catch(() => {});
    }
  });

  for (const cancellation of ['deadline', 'shutdown', 'ownership'] as const) {
    test(`${adapter} ${cancellation} rolls back and expires tx even while a handler ignores cancellation`, async (t) => {
      const app = await table(t);
      await pool.queryClassic(`INSERT INTO ${app} VALUES (0)`);
      const release = Promise.withResolvers<void>();
      let context: Context | undefined;
      const def = transactional(
        adapter,
        async (value, scope) => {
          if (value === 2) return;
          await scope.query(`UPDATE ${app} SET value = 1`);
          context = scope;
          await release.promise;
        },
        { maxAttempts: 1, executionTimeoutMs: cancellation === 'deadline' ? 200 : 30_000 },
      );
      const job = await jobs(pool).send(def, 1);
      const { stop, running } = start(def, { leaseMs: 300 });
      try {
        await until(async () => context !== undefined);
        const queued = await jobs(pool).send(def, 2);
        if (cancellation === 'shutdown') stop.abort();
        if (cancellation === 'ownership') {
          await pool.queryClassic(
            "UPDATE pgclear_jobs.jobs SET lease_until = clock_timestamp() - interval '1 second' WHERE id = $1",
            [job.id],
          );
          await recoverClaims(execute);
        }
        await until(async () => context!.signal.aborted);
        assert.deepEqual(
          (
            await pool.queryClassic(`UPDATE ${app} SET value = value RETURNING value`, [], {
              timeoutMs: 2000,
            })
          ).rows,
          [{ value: 0 }],
        );
        await assert.rejects(context!.query(`UPDATE ${app} SET value = 99`));
        assert.notEqual((await jobs(pool).get(job.id))?.state, 'completed');
        assert.equal((await jobs(pool).get(queued.id))?.state, 'pending');
        if (cancellation !== 'shutdown') {
          release.resolve();
          await until(async () => (await jobs(pool).get(queued.id))?.state === 'completed');
        }
        stop.abort();
        await running; // Shutdown must finish without waiting for the ignored handler.
      } finally {
        release.resolve();
        stop.abort();
        await running;
      }
    });
  }
}

for (const [commitDelivered, abortBeforeResponse] of [
  [false, false],
  [true, false],
  [true, true],
] as const) {
  test(
    `transactional worker reconciles a lost COMMIT response without replay (delivered: ${commitDelivered}, cancelled: ${abortBeforeResponse})`,
    { timeout: 15_000 },
    async (t) => {
      const app = await table(t);
      const proxy = await connectionProxy(url);
      const client = createPool({ ...proxy.connection, maxConnections: 1 });
      let calls = 0;
      let handlerSignal: AbortSignal | undefined;
      const def = defineJob({
        name: definition().name,
        transactional: true,
        async handler(_value: number, { tx, signal }) {
          calls++;
          handlerSignal = signal;
          await tx.queryClassic(`INSERT INTO ${app} VALUES (1)`);
        },
      });
      const worker = new Proxy(pool, {
        get(target, key) {
          if (key === 'transaction')
            return <T>(work: (tx: Transaction) => Promise<T>, options?: TransactionOptions) =>
              client.transaction(async (tx) => {
                const result = await work(tx);
                proxy.hold(commitDelivered ? 'responses' : 'requests');
                return result;
              }, options);
          const value: unknown = Reflect.get(target, key, target);
          return typeof value === 'function' ? value.bind(target) : value;
        },
      });
      const job = await jobs(pool).send(def, 1);
      const stop = new AbortController();
      const outcome = runJobs(worker, [def], { signal: stop.signal, shutdownTimeoutMs: 0 }).then(
        () => null,
        (error: unknown) => error,
      );
      try {
        await until(async () => proxy.buffered(commitDelivered ? 'responses' : 'requests') > 0);
        if (commitDelivered)
          await until(async () => (await jobs(pool).get(job.id))?.state === 'completed');
        if (abortBeforeResponse) {
          stop.abort();
          await until(async () => handlerSignal!.aborted);
        }
        proxy.disconnect();
        const error = await outcome;
        assert.ok(error instanceof DriverError);
        assert.equal(error.code, 'PGCLEAR_COMMIT_UNKNOWN');
        assert.equal(calls, 1);
        assert.equal(
          (await pool.queryClassic(`SELECT * FROM ${app}`)).rows.length,
          commitDelivered ? 1 : 0,
        );
        assert.equal(
          (await jobs(pool).get(job.id))?.state,
          commitDelivered ? 'completed' : 'running',
        );
        assert.equal(
          (await jobs(pool).attempts(job.id))[0]?.outcome,
          commitDelivered ? 'completed' : null,
        );
      } finally {
        stop.abort();
        proxy.release();
        await proxy.close();
        await outcome;
        await client.close();
      }
    },
  );
}

for (const adapter of ['pgclear', 'integration'] as const) {
  test(
    `${adapter} reports rollback response loss even after shutdown cancellation`,
    { timeout: 15_000 },
    async (t) => {
      const app = await table(t);
      const proxy = await connectionProxy(url);
      const client = createPool({ ...proxy.connection, maxConnections: 1 });
      let handlerSignal: AbortSignal | undefined;
      let calls = 0;
      const def = transactional(adapter, async (_value, { query, signal }) => {
        handlerSignal = signal;
        calls++;
        await query(`INSERT INTO ${app} VALUES (1)`);
        throw new JobError('permanent failure', { retry: false });
      });
      const worker = new Proxy(pool, {
        get(target, key) {
          if (key === 'transaction')
            return <T>(work: (tx: Transaction) => Promise<T>, options?: TransactionOptions) =>
              client.transaction(async (tx) => {
                try {
                  return await work(tx);
                } catch (error) {
                  proxy.hold('responses');
                  throw error;
                }
              }, options);
          const value: unknown = Reflect.get(target, key, target);
          return typeof value === 'function' ? value.bind(target) : value;
        },
      });
      const job = await jobs(pool).send(def, 1);
      const stop = new AbortController();
      const outcome = runners.get(def.name)!(worker, {
        signal: stop.signal,
        shutdownTimeoutMs: 0,
      }).then(
        () => null,
        (error: unknown) => error,
      );
      try {
        // PostgreSQL has executed ROLLBACK, but the driver has not received its reply.
        await until(async () => proxy.buffered('responses') > 0);
        stop.abort();
        await until(async () => handlerSignal!.aborted);
        proxy.disconnect();
        const error = await outcome;
        assert.ok(error instanceof AggregateError, 'a failed rollback must reach supervision');
        assert.ok(error.errors.some((cause: unknown) => cause instanceof DriverError));
        assert.match(String(error.errors[0]?.cause), /permanent failure/);
        assert.equal(calls, 1);
        assert.equal((await pool.queryClassic(`SELECT * FROM ${app}`)).rows.length, 0);
        assert.equal((await jobs(pool).get(job.id))?.state, 'running');
        assert.equal((await jobs(pool).attempts(job.id))[0]?.outcome, null);
      } finally {
        stop.abort();
        proxy.release();
        await proxy.close();
        await outcome;
        await client.close();
      }
    },
  );
}
