// Copy into an application root: node --env-file-if-exists=.env next-host.mjs ./worker.ts
// Build Next first. The worker sends { type: 'ready' } over IPC after startup validation.
// The parent owns process signals; each child owns and closes its own resources.
import { spawn } from 'node:child_process';
import { createRequire } from 'node:module';
import { resolve } from 'node:path';

const workerEntry = process.argv[2];
if (!workerEntry || process.argv.length !== 3)
  throw new Error('Usage: node next-host.mjs ./worker.ts');
const nextCli = createRequire(resolve('package.json')).resolve('next/dist/bin/next');
const children = new Set();
const lifetimes = [];
const failures = [];
let stopping = false;
let timer;

function stop() {
  if (stopping) return;
  stopping = true;
  for (const child of children) child.kill('SIGTERM');
  // Match this budget to the application's worker grace plus pool cleanup.
  timer = setTimeout(() => {
    if (children.size) {
      failures.push(new Error('Child shutdown exceeded 40 seconds.'));
      for (const child of children) child.kill('SIGKILL');
    }
  }, 40000);
  timer.unref();
}
function fail(error) {
  failures.push(error);
  stop();
}
function launch(name, args, ipc = false) {
  const child = spawn(process.execPath, args, {
    stdio: ipc ? ['inherit', 'inherit', 'inherit', 'ipc'] : 'inherit',
  });
  children.add(child);
  const done = new Promise((finish) => {
    child.once('error', fail);
    child.once('close', (code, signal) => {
      children.delete(child);
      if (!stopping) fail(new Error(`${name} exited unexpectedly (${signal ?? code}).`));
      // Next handles SIGTERM itself and exits with 128 + 15 rather than a signal status.
      else if (code !== 0 && signal !== 'SIGTERM' && !(name === 'Next' && code === 143))
        failures.push(new Error(`${name} failed during shutdown (${signal ?? code}).`));
      finish();
    });
  });
  lifetimes.push(done);
  return { child, done };
}
process.once('SIGINT', stop);
process.once('SIGTERM', stop);
try {
  const worker = launch('Worker', [resolve(workerEntry)], true);
  const ready = new Promise((finish) => {
    worker.child.on('message', (message) => {
      if (message && typeof message === 'object' && message.type === 'ready') finish();
    });
  });
  await Promise.race([ready, worker.done]);
  if (!stopping) launch('Next', [nextCli, 'start', '--hostname', '127.0.0.1']);
  await Promise.all(lifetimes);
} catch (error) {
  fail(error);
} finally {
  stop();
  await Promise.all(lifetimes);
  clearTimeout(timer);
  process.removeListener('SIGINT', stop);
  process.removeListener('SIGTERM', stop);
}
if (failures.length) {
  console.error(failures.length === 1 ? failures[0] : new AggregateError(failures, 'Host failed.'));
  process.exitCode = 1;
}
