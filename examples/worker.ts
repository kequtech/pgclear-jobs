import { DriverError } from '@kequtech/pgclear';
import { defineWorker, JobError, runJobs } from '@kequtech/pgclear-jobs';
import { connect } from './database.ts';
import { greeting } from './jobs.ts';

const worker = defineWorker(greeting, {
  handler(payload, { signal }) {
    signal.throwIfAborted();
    if (payload.name === 'fail') throw new JobError('Example permanent failure', { retry: false });
    console.log(`Hello, ${payload.name}`);
  },
});
const pool = connect();
const stop = new AbortController();
process.once('SIGTERM', () => stop.abort());
process.once('SIGINT', () => stop.abort());
try {
  await runJobs(pool, [worker], {
    signal: stop.signal,
    onReady() {
      console.log('Worker ready');
    },
    onEvent(event) {
      console.log(event.type, 'id' in event ? event.id : '');
    },
    onHandlerError(failure) {
      // Explicit application policy: escalate an uncertain commit to supervision.
      return failure.error instanceof DriverError && failure.error.code === 'PGCLEAR_COMMIT_UNKNOWN'
        ? 'stop'
        : 'continue';
    },
  });
} catch (error) {
  console.error('Worker failed', error);
  process.exitCode = 1;
} finally {
  await pool.close();
}
