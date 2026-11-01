#!/usr/bin/env node
import { createPool } from '@kequtech/pgclear';
import { migrateJobs } from './schema.js';

const usage = 'Usage: pgclear-jobs migrate [up|down]';

async function main(): Promise<void> {
  const args = process.argv.slice(2);
  if (args.length === 1 && (args[0] === '--help' || args[0] === '-h')) {
    console.log(`${usage}\nReads DATABASE_URL from the environment or .env.`);
    return;
  }
  const [command, direction = 'up', ...rest] = args;
  if (command !== 'migrate' || rest.length || (direction !== 'up' && direction !== 'down'))
    throw new TypeError(usage);
  try {
    process.loadEnvFile();
  } catch (error) {
    if (!(error instanceof Error && 'code' in error && error.code === 'ENOENT')) throw error;
  }
  const connectionString = process.env.DATABASE_URL;
  if (!connectionString) throw new Error('Set DATABASE_URL in the environment or .env.');
  const pool = createPool({ connectionString });
  try {
    await migrateJobs(pool, direction);
    console.log(direction === 'up' ? 'Jobs schema is ready.' : 'Jobs schema removed.');
  } finally {
    await pool.close();
  }
}

main().catch((error: unknown) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
