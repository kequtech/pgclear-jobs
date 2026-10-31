import { rm } from 'node:fs/promises';
import { spawnSync } from 'node:child_process';

const test = process.argv[2] === '--test';
if (process.argv.slice(2).some((argument) => argument !== '--test')) {
  throw new Error('Unknown build argument.');
}
await rm(test ? '.test-dist' : 'dist', { recursive: true, force: true });
const result = spawnSync(
  process.execPath,
  ['node_modules/typescript/bin/tsc', '-p', test ? 'tsconfig.test.json' : 'tsconfig.build.json'],
  { stdio: 'inherit' },
);
if (result.error) throw result.error;
process.exitCode = result.status ?? 1;
