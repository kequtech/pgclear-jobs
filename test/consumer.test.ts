import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';

test('packed core and declarations work without the optional Tenon peer', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'tenon-jobs-consumer-'));
  try {
    // Pack installed runtime dependencies too: a fresh npm cache has no registry metadata.
    const locations = execFileSync('npm', ['ls', '--omit=dev', '--all', '--parseable'], {
      encoding: 'utf8',
    })
      .trim()
      .split('\n');
    const archives = locations.map((location) => {
      const result = JSON.parse(
        execFileSync(
          'npm',
          ['pack', location, '--ignore-scripts', '--json', '--pack-destination', directory],
          { encoding: 'utf8' },
        ),
      ) as { filename: string }[];
      return join(directory, result[0]!.filename);
    });
    await writeFile(
      join(directory, 'package.json'),
      JSON.stringify({ private: true, type: 'module' }),
    );
    execFileSync(
      'npm',
      [
        'install',
        '--prefix',
        directory,
        '--offline',
        '--cache',
        join(directory, 'npm-cache'),
        '--ignore-scripts',
        '--no-audit',
        '--no-fund',
        ...archives,
      ],
      { stdio: 'pipe' },
    );
    await assert.rejects(
      readFile(join(directory, 'node_modules/@kequtech/tenon-orm/package.json')),
    );
    await writeFile(
      join(directory, 'consumer.mjs'),
      `
      import { defineJob, jobs, runJobs, installJobs } from '@kequtech/tenon-jobs';
      import { createPool } from '@kequtech/pgclear';
      const definition = defineJob({ name: 'consumer', handler(payload) {} });
      const pool = createPool({});
      if (!definition.name || typeof jobs(pool).send !== 'function' ||
          typeof runJobs !== 'function' || typeof installJobs !== 'function') throw new Error('Invalid exports');
      await pool.close();
    `,
    );
    execFileSync(process.execPath, [join(directory, 'consumer.mjs')], { stdio: 'pipe' });
    await writeFile(
      join(directory, 'consumer.ts'),
      `
      import { defineJob, jobs, runJobs } from '@kequtech/tenon-jobs';
      import { createPool } from '@kequtech/pgclear';
      const pool = createPool({});
      const definition = defineJob({ name: 'typed', handler(payload: { id: string }) {} });
      const record = await jobs(pool).send(definition, { id: 'example' });
      const id: string = record.payload.id;
      const batch = await jobs(pool).sendMany(definition, [{ payload: { id: 'batch' } }]);
      const updated = await jobs(pool).update(definition, record.id, { id: 'latest' });
      const requested = await jobs(pool).request(definition, { id: 'follow-up' }, { key: 'one' });
      const requestedId: string = requested.payload.id;
      const blockedBy: string | null = requested.blockedBy;
      void requestedId;
      void blockedBy;
      // @ts-expect-error Requests preserve shipped definition types.
      await jobs(pool).request(definition, { id: false }, { key: 'one' });
      // @ts-expect-error Events have stable IDs; keyed work uses request.
      await jobs(pool).send(definition, { id: 'event' }, { key: 'one' });
      void runJobs(pool, [definition], { retentionMs: false });
      const cursor: string = updated.cursor;
      const retryOf: string | null = updated.retryOf;
      await jobs(pool).find(definition, { after: cursor });
      // @ts-expect-error Batch payloads cannot widen the shipped definition.
      await jobs(pool).sendMany(definition, [{ payload: { id: false } }]);
      void batch;
      void retryOf;
      // @ts-expect-error Invalid payload must fail against shipped declarations.
      await jobs(pool).send(definition, { id: 1 });
      void runJobs(pool, [definition]);
      void id;
    `,
    );
    execFileSync(
      process.execPath,
      [
        'node_modules/typescript/bin/tsc',
        '--ignoreConfig',
        '--noEmit',
        '--strict',
        '--target',
        'ES2024',
        '--module',
        'NodeNext',
        '--typeRoots',
        join(process.cwd(), 'node_modules/@types'),
        '--types',
        'node',
        join(directory, 'consumer.ts'),
      ],
      { stdio: 'pipe' },
    );
  } catch (error) {
    if (error instanceof Error && 'stderr' in error && Buffer.isBuffer(error.stderr)) {
      const stdout =
        'stdout' in error && Buffer.isBuffer(error.stdout) ? error.stdout.toString() : '';
      throw new Error(`${error.message}\n${stdout}\n${error.stderr.toString()}`, { cause: error });
    }
    throw error;
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
