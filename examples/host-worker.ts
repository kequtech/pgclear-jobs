// Copy outside node_modules and replace the example worker with application bindings.
// next-host.mjs starts this entry point with an IPC channel; it also runs standalone.
import { defineJob, defineWorker, startJobs } from '@kequtech/pgclear-jobs';
import { createPool } from '@kequtech/pgclear';

const connection = process.env.DATABASE_URL;
if (!connection) throw new Error('DATABASE_URL is required.');
const pool = createPool(connection);
const stop = new AbortController();
const shutdown = () => stop.abort();
process.once('SIGTERM', shutdown);
process.once('SIGINT', shutdown);
process.once('disconnect', shutdown);
const spec = defineJob<{ value: number }>({ name: 'example:query' });
const worker = defineWorker(spec, {
  transactional: true,
  async handler({ value }, { tx }) {
    await tx.queryClassic('SELECT $1 AS value', [value]);
  },
});
const running = startJobs(pool, [worker], { signal: stop.signal });
const failures: unknown[] = [];
const record = (error: unknown) => {
  if (!failures.includes(error)) failures.push(error);
};
try {
  await running.ready;
  process.send?.({ type: 'ready' });
  await running.done;
} catch (error) {
  record(error);
} finally {
  shutdown();
  try {
    await running.done;
  } catch (error) {
    record(error);
  }
  try {
    await pool.close();
  } catch (error) {
    record(error);
  }
  process.removeListener('SIGTERM', shutdown);
  process.removeListener('SIGINT', shutdown);
  process.removeListener('disconnect', shutdown);
  if (process.connected) process.disconnect();
}
if (failures.length === 1) throw failures[0];
if (failures.length > 1) throw new AggregateError(failures, 'Worker cleanup failed.');
