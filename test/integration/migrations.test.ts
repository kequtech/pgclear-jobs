import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';
import test from 'node:test';
import { jobs, installJobs, migrateJobs } from '../../src/index.js';
import { jobsMigration, jobsUpgrade } from '../../src/migrations.js';
import { checkSchema } from '../../src/schema.js';
import { claimJob, finishClaim } from '../../src/claims.js';
import { definition, execute, pool, url } from '../helpers/database.js';

const run = promisify(execFile);
const cli = fileURLToPath(new URL('../../src/cli.js', import.meta.url));

test('version 1 upgrades preserve outstanding work and match fresh storage; reversal protects keyed schedules', async () => {
  try {
    await applySql(jobsMigration.down);
    await applySql(await readFile('test/fixtures/schema-v1.sql', 'utf8'));
    const def = definition({ retryDelayMs: 60000 });
    const head = await jobs(pool).request(def, 1, { key: 'upgrade' });
    await claimJob(execute, [def.name], 300000);
    await jobs(pool).request(def, 2, { key: 'upgrade' });
    const retryDef = definition({ retryDelayMs: 60000 });
    await jobs(pool).send(retryDef, 3);
    await finishClaim(execute, (await claimJob(execute, [retryDef.name], 30000)).claim!, 'retry');
    await jobs(pool).send(def, 4, { delayMs: 60000 });
    await pool.queryClassic(
      `INSERT INTO pgclear_jobs.schedules(id,name,payload,policy,cron,time_zone,next_run_at)
      SELECT $1, name, payload, policy, '* * * * *', 'UTC', clock_timestamp() + interval '1 day'
      FROM pgclear_jobs.jobs WHERE id = $2`,
      [randomUUID(), head.id],
    );
    await assert.rejects(checkSchema(execute), /incompatible/);
    await assert.rejects(migrateJobs(pool), /upgrade migration/);
    const before = await snapshot();
    await assert.rejects(applySql(jobsUpgrade.up + '\nSELECT 1/0;'));
    assert.deepEqual(await snapshot(), before);
    await applySql(jobsUpgrade.up);
    await checkSchema(execute);
    const upgraded = (await snapshot()) as { schedules: Record<string, unknown>[] };
    assert.deepEqual(
      { ...upgraded, schedules: upgraded.schedules.map(({ request_key: _, ...s }) => s) },
      before,
    );
    await assert.rejects(applySql(jobsUpgrade.up), /Expected Jobs storage version 1/);
    const catalog = async () =>
      (
        await pool.queryClassic(`SELECT column_name, data_type, is_nullable, column_default
      FROM information_schema.columns WHERE table_schema='pgclear_jobs' ORDER BY table_name, ordinal_position`)
      ).rows;
    const upgradedCatalog = await catalog();
    await jobs(pool).schedule(def, 9, { cron: '* * * * *', requestKey: 'upgrade' });
    await assert.rejects(applySql(jobsUpgrade.down), /Remove or convert/);
    await jobs(pool).unschedule(def);
    await applySql(jobsUpgrade.down);
    await assert.rejects(checkSchema(execute), /incompatible/);
    await applySql(jobsUpgrade.up);
    await applySql(jobsMigration.down);
    await applySql(jobsMigration.up);
    assert.deepEqual(await catalog(), upgradedCatalog);
  } finally {
    await migrateJobs(pool, 'down');
    await installJobs(pool);
  }
});

async function snapshot() {
  const result = await pool.queryClassic<{ record: unknown }>(`SELECT jsonb_build_object(
    'jobs', (SELECT jsonb_agg(to_jsonb(j) ORDER BY id) FROM pgclear_jobs.jobs j),
    'attempts', (SELECT jsonb_agg(to_jsonb(a) ORDER BY job_id, number) FROM pgclear_jobs.attempts a),
    'schedules', (SELECT jsonb_agg(to_jsonb(s) ORDER BY id) FROM pgclear_jobs.schedules s)
  ) AS record`);
  return result.rows[0]!.record;
}

async function absent() {
  const { rows } = await pool.queryClassic<{ absent: boolean }>(
    "SELECT to_regnamespace('pgclear_jobs') IS NULL AS absent",
  );
  return rows[0]!.absent;
}

async function applySql(sql: string) {
  const session = await pool.acquire();
  try {
    await session.queryClassic('BEGIN');
    try {
      await session.script(sql);
      await session.queryClassic('COMMIT');
    } catch (error) {
      await session.queryClassic('ROLLBACK');
      throw error;
    }
  } finally {
    await session.release();
  }
}

test('repeated and concurrent migrations leave pending, running, retrying and blocked work intact', async () => {
  const headDef = definition();
  const head = await jobs(pool).request(headDef, 1, { key: 'entity' });
  await claimJob(execute, [headDef.name], 300_000);
  const successor = await jobs(pool).request(headDef, 2, { key: 'entity' });
  const retryDef = definition({ retryDelayMs: 60_000 });
  await jobs(pool).send(retryDef, 3);
  await finishClaim(
    execute,
    (await claimJob(execute, [retryDef.name], 30_000)).claim!,
    'temporary',
  );
  await jobs(pool).send(headDef, 4, { delayMs: 60_000 });
  await jobs(pool).schedule(headDef, 5, { cron: '* * * * *' });
  const before = await snapshot();
  await Promise.all([migrateJobs(pool), installJobs(pool), migrateJobs(pool)]);
  await checkSchema(execute);
  assert.deepEqual(await snapshot(), before);
  assert.equal((await jobs(pool).get(successor.id))?.blockedBy, head.id);
});

test('the command installs from .env, removes populated storage and can install again', async (t) => {
  const directory = await mkdtemp(join(tmpdir(), 'pgclear-jobs-cli-'));
  t.after(() => rm(directory, { recursive: true, force: true }));
  await writeFile(join(directory, '.env'), `DATABASE_URL=${url}\n`);
  const env = { ...process.env };
  delete env.DATABASE_URL;
  const command = (...args: string[]) =>
    run(process.execPath, [cli, 'migrate', ...args], { cwd: directory, env });
  const app = `public.app_${randomUUID().replaceAll('-', '')}`;
  await pool.queryClassic(`CREATE TABLE ${app} (value integer NOT NULL)`);
  await pool.queryClassic(`INSERT INTO ${app} VALUES (42)`);
  t.after(() => pool.queryClassic(`DROP TABLE ${app}`));
  const job = await jobs(pool).send(definition(), 1);
  try {
    await command();
    assert.equal((await jobs(pool).get(job.id))?.state, 'pending');
    await command('down');
    assert.equal(await absent(), true);
    assert.deepEqual((await pool.queryClassic(`SELECT * FROM ${app}`)).rows, [{ value: 42 }]);
    await assert.rejects(checkSchema(execute), /schema is incompatible/);
    await command('down');
    await command('up');
    await checkSchema(execute);
    assert.equal((await jobs(pool).send(definition(), 2)).state, 'pending');
  } finally {
    await installJobs(pool);
  }
});

test('one exported up/down pair works as ordinary application migration files', async () => {
  try {
    await applySql(jobsMigration.down);
    assert.equal(await absent(), true);
    await applySql(jobsMigration.up);
    await checkSchema(execute);
    const def = definition();
    await jobs(pool).send(def, 1);
    const before = await snapshot();
    await assert.rejects(applySql(jobsMigration.up));
    assert.deepEqual(await snapshot(), before);
    await applySql(jobsMigration.down);
    await applySql(jobsMigration.up);
    assert.equal((await jobs(pool).stats(def)).pending, 0);
  } finally {
    await installJobs(pool);
  }
});

test('a failed application migration rolls back installation or complete namespace removal', async () => {
  await jobs(pool).send(definition(), 1);
  const before = await snapshot();
  try {
    await assert.rejects(applySql(jobsMigration.down + '\nSELECT 1 / 0;'));
    await checkSchema(execute);
    assert.deepEqual(await snapshot(), before);
    await migrateJobs(pool, 'down');
    await assert.rejects(applySql(jobsMigration.up + '\nSELECT 1 / 0;'));
    assert.equal(await absent(), true);
    await Promise.all([migrateJobs(pool), installJobs(pool)]);
    await checkSchema(execute);
  } finally {
    await installJobs(pool);
  }
});

test('an incompatible schema requires intentional down/up; invalid directions fail before SQL', async () => {
  const original = await jobs(pool).send(definition(), 1);
  try {
    await pool.queryClassic('UPDATE pgclear_jobs.schema_version SET version = 99');
    await assert.rejects(migrateJobs(pool), /schema is incompatible/);
    assert.equal((await jobs(pool).get(original.id))?.payload, 1);
    await migrateJobs(pool, 'down');
    await migrateJobs(pool);
    await checkSchema(execute);
    assert.equal(await jobs(pool).get(original.id), null);
    const unused = {
      transaction: async () => {
        throw new Error('must not acquire');
      },
    };
    // @ts-expect-error Reject obsolete version targeting before SQL.
    await assert.rejects(migrateJobs(unused, { toVersion: 0 }), TypeError);
  } finally {
    await migrateJobs(pool, 'down');
    await installJobs(pool);
  }
});
