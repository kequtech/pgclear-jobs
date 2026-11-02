import { createPool } from '@kequtech/pgclear';
import { defineJob, jobs, runJobs } from '../src/index.js';
import { createJobsIntegration } from '../src/integration.js';
import type { Execute } from '../src/integration.js';

interface Scope {
  readonly execute: Execute;
}
interface Database extends Scope {
  readonly pool: ReturnType<typeof createPool>;
}

// Compile-only: each API binds its own transaction type, while records retain payload types.
async function types(database: Database) {
  const api = createJobsIntegration<Database, Scope>({
    pool: (db) => db.pool,
    execute: (scope) => scope.execute,
    transaction: (db, work, options) =>
      db.pool.transaction(
        (tx) => work({ execute: (text, values) => tx.queryClassic(text, values) }),
        options,
      ),
  });
  const shared = defineJob<{ id: string }>({ name: 'shared' });
  const bound = api.defineWorker(shared, {
    transactional: true,
    async handler(payload, { tx }) {
      await tx.execute('SELECT $1::text', [payload.id]);
      // @ts-expect-error Bound ORM workers do not receive the native driver transaction.
      await tx.queryClassic('SELECT 1');
    },
  });
  await api.jobs(database).send(shared, { id: 'one' });
  await api.runJobs(database, [bound]);
  // @ts-expect-error Native runners cannot invoke integrated worker scopes.
  await runJobs(database.pool, [bound]);
  const custom = api.defineJob({
    name: 'custom',
    transactional: true,
    async handler(payload: { id: string }, { tx }) {
      await tx.execute('SELECT $1::text', [payload.id]);
      // @ts-expect-error The adapter's scope is not a pgclear transaction.
      await tx.queryClassic('SELECT 1');
    },
  });
  const native = defineJob({
    name: 'native',
    transactional: true,
    async handler(_payload: number, { tx }) {
      await tx.queryClassic('SELECT 1');
    },
  });
  await api.jobs(database).send(custom, { id: 'one' });
  await jobs(database.pool).send(custom, { id: 'two' });
  // @ts-expect-error Record inputs cannot widen the integration definition's payload.
  await api.jobs(database).send(custom, { id: 1 });
  await api.runJobs(database, [custom]);
  // @ts-expect-error A native runner must not invoke a custom handler with a driver scope.
  await runJobs(database.pool, [custom]);
  // @ts-expect-error A custom runner must not invoke a native transactional handler.
  await api.runJobs(database, [native]);
}
void types;
