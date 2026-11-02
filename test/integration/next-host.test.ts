import assert from 'node:assert/strict';
import { spawn, execFile } from 'node:child_process';
import { once } from 'node:events';
import { cp, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { createServer } from 'node:net';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { test } from 'node:test';
import { setTimeout as delay } from 'node:timers/promises';
import { promisify } from 'node:util';
import { createPool } from '@kequtech/pgclear';
import { url } from '../helpers/database.js';

const run = promisify(execFile);
async function freePort() {
  const server = createServer().listen(0, '127.0.0.1');
  await once(server, 'listening');
  const address = server.address();
  assert.ok(address && typeof address !== 'string');
  const port = address.port;
  await new Promise<void>((resolve, reject) =>
    server.close((error) => (error ? reject(error) : resolve())),
  );
  return port;
}

// The framework is a disposable consumer dependency, never part of the library dependency graph.
// The runner/transport cases live in Jobs tests; this verifies actual Next process exit behavior.
test(
  'the Next host supervises startup, both process signals and child failures',
  { timeout: 240000 },
  async (t) => {
    const directory = await mkdtemp(join(tmpdir(), 'jobs-next-'));
    const pool = createPool(url);
    const database = `jobs_next_${Date.now()}`;
    await pool.queryClassic(`CREATE DATABASE "${database}"`);
    const connection = new URL(url);
    connection.pathname = `/${database}`;
    const storage = createPool({ connectionString: connection.href });
    try {
      // Install storage in a private database; the configured application's queue is untouched.
      const { jobsMigration } = await import('../../src/migrations.js');
      const session = await storage.acquire();
      try {
        await session.script(jobsMigration.up);
      } finally {
        await session.release();
      }
      await cp('test/fixtures/next', directory, { recursive: true });
      await cp('examples/next-host.mjs', join(directory, 'next-host.mjs'));
      await cp('examples/host-worker.ts', join(directory, 'worker.ts'));
      const packed = await run('npm', [
        'pack',
        '--ignore-scripts',
        '--json',
        '--pack-destination',
        directory,
      ]);
      const archive = (JSON.parse(packed.stdout) as { filename: string }[])[0]!.filename;
      const manifest = JSON.parse(await readFile(join(directory, 'package.json'), 'utf8')) as {
        dependencies: Record<string, string>;
        overrides?: Record<string, string>;
      };
      manifest.dependencies['@kequtech/pgclear-jobs'] = `file:${join(directory, archive)}`;
      // Install the actual peer artifact beside Jobs; no Tenon package is involved.
      const driverRoot = resolve(import.meta.dirname, '../../../node_modules/@kequtech/pgclear');
      const driverPacked = await run('npm', [
        'pack',
        driverRoot,
        '--ignore-scripts',
        '--json',
        '--pack-destination',
        directory,
      ]);
      const driverArchive = (JSON.parse(driverPacked.stdout) as { filename: string }[])[0]!
        .filename;
      manifest.dependencies['@kequtech/pgclear'] = `file:${join(directory, driverArchive)}`;
      await writeFile(join(directory, 'package.json'), JSON.stringify(manifest));
      await run(
        'npm',
        ['install', '--prefer-offline', '--ignore-scripts', '--no-audit', '--no-fund'],
        { cwd: directory, timeout: 120000 },
      );
      await assert.rejects(
        readFile(join(directory, 'node_modules/@kequtech/tenon-orm/package.json')),
      );
      const next = join(directory, 'node_modules/next/dist/bin/next');
      await run(process.execPath, [next, 'build', '--webpack'], {
        cwd: directory,
        timeout: 120000,
        env: { ...process.env, NEXT_TELEMETRY_DISABLED: '1' },
      });
      for (const signal of ['SIGINT', 'SIGTERM'] as const)
        await t.test(`${signal} waits for worker and framework cleanup`, async () => {
          const port = await freePort();
          const child = spawn(process.execPath, ['next-host.mjs', './worker.ts'], {
            cwd: directory,
            env: {
              ...process.env,
              DATABASE_URL: connection.href,
              PORT: String(port),
              NEXT_TELEMETRY_DISABLED: '1',
            },
            stdio: ['ignore', 'pipe', 'pipe'],
          });
          let output = '';
          child.stdout.on('data', (data) => {
            output += data;
          });
          child.stderr.on('data', (data) => {
            output += data;
          });
          const exited = once(child, 'exit');
          try {
            const deadline = Date.now() + 20000;
            for (;;) {
              assert.equal(child.exitCode, null, output);
              try {
                const response = await fetch(`http://127.0.0.1:${port}`);
                if (response.status === 200) {
                  await response.text();
                  break;
                }
              } catch {
                /* Wait for actual HTTP readiness. */
              }
              if (Date.now() > deadline) throw new Error(`Next did not become ready: ${output}`);
              await delay(25);
            }
            child.kill(signal);
            const [code, killed] = await exited;
            assert.equal(killed, null, output);
            assert.equal(code, 0, output);
          } finally {
            if (child.exitCode === null) {
              child.kill('SIGTERM');
              await exited;
            }
          }
        });
      for (const failure of ['worker', 'framework'] as const)
        await t.test(`${failure} failure stops the service`, async () => {
          if (failure === 'worker')
            await writeFile(join(directory, 'failed-worker.mjs'), 'process.exit(7);');
          const child = spawn(
            process.execPath,
            ['next-host.mjs', failure === 'worker' ? './failed-worker.mjs' : './worker.ts'],
            {
              cwd: directory,
              env: {
                ...process.env,
                DATABASE_URL: connection.href,
                PORT: 'invalid',
                NEXT_TELEMETRY_DISABLED: '1',
              },
              stdio: 'pipe',
            },
          );
          child.stdout.resume();
          child.stderr.resume();
          const [code, signal] = await once(child, 'exit');
          assert.equal(signal, null);
          assert.equal(code, 1);
        });
    } finally {
      await storage.close();
      try {
        await pool.queryClassic(`DROP DATABASE "${database}" WITH (FORCE)`);
      } finally {
        await pool.close();
      }
      await rm(directory, { recursive: true, force: true });
    }
  },
);
