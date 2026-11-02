import { randomUUID } from 'node:crypto';
import { jobs } from '@kequtech/pgclear-jobs';
import { connect } from './database.ts';
import { greeting } from './jobs.ts';

const [name = 'Ada', id = randomUUID()] = process.argv.slice(2);
const pool = connect();
try {
  const job = await jobs(pool).send(greeting, { name }, { id });
  console.log(JSON.stringify({ id: job.id, state: job.state }));
} finally {
  await pool.close();
}
