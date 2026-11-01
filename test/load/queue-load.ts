import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { writeFile } from 'node:fs/promises';
import { availableParallelism, cpus } from 'node:os';
import { monitorEventLoopDelay } from 'node:perf_hooks';
import { setTimeout as delay } from 'node:timers/promises';
import { createPool } from '@kequtech/pgclear';
import { defineJob, installJobs, jobs, runJobs } from '../../src/index.js';
import { claimJob } from '../../src/claims.js';
import { executionPolicy } from '../../src/definition.js';
import { pruneCompleted } from '../../src/retention.js';
import type { Execute } from '../../src/storage.js';

const configured = process.env.PGCLEAR_JOBS_TEST_DATABASE_URL;
if (!configured || !new URL(configured).pathname.endsWith('_test'))
  throw new Error('Use a dedicated database ending in _test.');
const url = configured;
const durationMs = Number(process.env.PGCLEAR_JOBS_LOAD_DURATION_MS ?? 10_000);
const intervalMs = Number(process.env.PGCLEAR_JOBS_LOAD_INTERVAL_MS ?? 50);
if (!Number.isSafeInteger(durationMs) || durationMs < 1000 || durationMs > 300_000)
  throw new Error('Load duration must be 1000–300000 milliseconds.');
if (!Number.isSafeInteger(intervalMs) || intervalMs < 10 || intervalMs > 1000)
  throw new Error('Load interval must be 10–1000 milliseconds.');
const database = createPool({ connectionString: url, maxConnections: 4 });
await installJobs(database);

function percentile(values: readonly number[], fraction: number) {
  if (!values.length) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  return (
    Math.round(sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * fraction))]! * 100) /
    100
  );
}

interface Payload {
  load: 'active';
  kind: number;
  sequence: number;
  queuedAt: number;
  retry: boolean;
}

async function profile(label: string, poolSize: number, handlerDbMs: number, appDbMs: number) {
  const client = createPool({ connectionString: url, maxConnections: poolSize });
  const stop = new AbortController();
  const loop = monitorEventLoopDelay({ resolution: 20 });
  const failures: unknown[] = [];
  const calls = new Map<string, number>();
  const retryIds = new Set<string>();
  const starts: number[][] = [[], [], [], []];
  const submitTimes: number[] = [],
    appTimes: number[] = [];
  let submitted = 0,
    completed = 0,
    appOperations = 0,
    appDone = false;
  let peakWaiting = 0,
    peakConnections = 0,
    peakOutstanding = 0;
  let runners: Promise<void>[] = [],
    applications: Promise<void>[] = [];
  let sampler: ReturnType<typeof setInterval> | undefined;
  const definitions = Array.from({ length: 4 }, (_, kind) =>
    defineJob({
      name: `load-${randomUUID()}`,
      maxAttempts: 2,
      retryDelayMs: 10,
      async handler(payload: Payload, context) {
        starts[kind]!.push(performance.now() - payload.queuedAt);
        calls.set(context.id, (calls.get(context.id) ?? 0) + 1);
        await client.queryClassic('SELECT pg_sleep($1::double precision)', [handlerDbMs / 1000], {
          signal: context.signal,
        });
        await delay(2, undefined, { signal: context.signal });
        if (payload.retry && context.attempt === 1) throw new Error('Expected load-test retry');
        completed++;
      },
    }),
  );
  const names = definitions.map((definition) => definition.name);
  const namesJson = JSON.stringify(names);
  const plans: Record<string, unknown> = {};
  const capturePlan = async (name: string, work: (execute: Execute) => Promise<unknown>) => {
    const rollback = new Error('rollback EXPLAIN');
    try {
      await database.transaction(async (tx) => {
        await work(async (sql, values) => {
          const { rows } = await tx.queryClassic<{
            'QUERY PLAN': { Plan: unknown; 'Execution Time': number }[];
          }>(`EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ${sql}`, values);
          const plan = rows[0]!['QUERY PLAN'][0]!;
          const nodes: { type: unknown; index?: unknown; rows: unknown; buffers: unknown }[] = [];
          const visit = (node: unknown) => {
            const record = node as Record<string, unknown>;
            nodes.push({
              type: record['Node Type'],
              ...(record['Index Name'] ? { index: record['Index Name'] } : {}),
              rows: record['Actual Rows'],
              buffers: record['Shared Hit Blocks'],
            });
            for (const child of (record.Plans ?? []) as unknown[]) visit(child);
          };
          visit(plan.Plan);
          plans[name] = { executionMs: plan['Execution Time'], nodes };
          return { rows: [] };
        });
        throw rollback;
      });
    } catch (error) {
      if (error !== rollback) throw error;
    }
  };
  const send = async (kind: number, count: number, sequence: number) => {
    const inputs = Array.from({ length: count }, (_, index) => {
      const id = randomUUID();
      const retry = kind === 2 && (sequence + index) % 3 === 0;
      if (retry) retryIds.add(id);
      return {
        id,
        payload: {
          load: 'active' as const,
          kind,
          sequence: sequence + index,
          queuedAt: performance.now(),
          retry,
        },
        ...(kind === 3 ? { delayMs: 200 } : {}),
      };
    });
    const start = performance.now();
    const inserted = await jobs(client).sendMany(definitions[kind]!, inputs);
    assert.equal(inserted.length, count);
    submitted += count;
    submitTimes.push(performance.now() - start);
  };
  try {
    for (const definition of definitions) {
      for (const [count, days] of [
        [5000, 3],
        [750, 10],
      ] as const) {
        await database.queryClassic(
          `INSERT INTO pgclear_jobs.jobs (id, name, payload, policy, state, finished_at)
           SELECT gen_random_uuid(), $1, '{"load":"history"}'::jsonb, $2::jsonb, 'completed',
           clock_timestamp() - $3::double precision * interval '1 day' FROM generate_series(1, $4::integer)`,
          [definition.name, JSON.stringify(executionPolicy(definition)), days, count],
        );
      }
    }
    for (let index = 0; index < 1500; index += 500) await send(0, 500, index);
    await database.queryClassic('ANALYZE pgclear_jobs.jobs');
    await capturePlan('claim', (execute) => claimJob(execute, names, 5000));
    await capturePlan('cleanup', (execute) => pruneCompleted(execute, names, 7 * 86_400_000));
    loop.enable();
    sampler = setInterval(() => {
      peakWaiting = Math.max(peakWaiting, client.stats.waiting);
      peakConnections = Math.max(peakConnections, client.stats.total);
      peakOutstanding = Math.max(peakOutstanding, submitted - completed);
    }, 10);
    const started = performance.now();
    const fail = (error: unknown) => {
      failures.push(error);
      stop.abort();
      appDone = true;
    };
    runners = Array.from({ length: 2 }, () =>
      runJobs(client, definitions, {
        signal: stop.signal,
        concurrency: 4,
        leaseMs: 5000,
        databaseTimeoutMs: 5000,
        pollIntervalMs: 10,
        recoveryIntervalMs: 100,
        shutdownTimeoutMs: 1000,
      }).catch(fail),
    );
    applications = Array.from({ length: 2 }, () =>
      (async () => {
        while (!appDone) {
          const before = performance.now();
          await client.transaction(async (tx) => {
            await tx.queryClassic('SELECT pg_sleep($1::double precision)', [appDbMs / 1000]);
          });
          appTimes.push(performance.now() - before);
          appOperations++;
          await delay(20);
        }
      })().catch(fail),
    );
    let sequence = 1500;
    while (performance.now() - started < durationMs && !failures.length) {
      const before = performance.now();
      await Promise.all([
        send(0, 8, sequence),
        send(1, 1, sequence),
        send(2, 1, sequence),
        send(3, 1, sequence),
      ]);
      sequence++;
      await delay(Math.max(0, intervalMs - (performance.now() - before)));
    }
    const producingMs = performance.now() - started;
    const drainStart = performance.now();
    while (true) {
      if (failures.length) throw new AggregateError(failures, 'Load workers failed');
      const { rows } = await database.queryClassic<{ outstanding: number }>(
        `SELECT count(*)::integer AS outstanding FROM pgclear_jobs.jobs WHERE name IN
         (SELECT jsonb_array_elements_text($1::jsonb)) AND state IN ('pending','running')`,
        [namesJson],
      );
      if (rows[0]!.outstanding === 0) break;
      if (performance.now() - drainStart > 60_000) throw new Error('Load backlog did not drain.');
      await delay(100);
    }
    const elapsedMs = performance.now() - started;
    appDone = true;
    stop.abort();
    await Promise.all([...runners, ...applications]);
    if (failures.length) throw new AggregateError(failures, 'Load workers failed');
    assert.equal(calls.size, submitted);
    assert.equal(completed, submitted);
    for (const [id, count] of calls) assert.equal(count, retryIds.has(id) ? 2 : 1);
    const { rows } = await database.queryClassic<{
      expired: number;
      failed: number;
      oldHistory: number;
    }>(
      `SELECT (SELECT count(*)::integer FROM pgclear_jobs.attempts a JOIN pgclear_jobs.jobs j ON j.id = a.job_id
         WHERE j.name IN (SELECT jsonb_array_elements_text($1::jsonb)) AND a.outcome = 'expired') AS expired,
       count(*) FILTER (WHERE state = 'failed')::integer AS failed,
       count(*) FILTER (WHERE payload->>'load' = 'history' AND finished_at < clock_timestamp() - interval '7 days')::integer AS "oldHistory"
       FROM pgclear_jobs.jobs WHERE name IN (SELECT jsonb_array_elements_text($1::jsonb))`,
      [namesJson],
    );
    assert.equal(rows[0]!.expired, 0);
    assert.equal(rows[0]!.failed, 0);
    return {
      profile: label,
      queryPoolSize: poolSize,
      runners: 2,
      concurrencyPerRunner: 4,
      handlerDbMs,
      applicationDbMs: appDbMs,
      initialBacklog: 1500,
      retainedHistory: 20_000,
      initialExpiredHistory: 3000,
      submitted,
      retries: retryIds.size,
      producingMs: Math.round(producingMs),
      totalMs: Math.round(elapsedMs),
      throughputPerSecond: Math.round((submitted / elapsedMs) * 1000),
      peakOutstanding,
      peakWaiting,
      peakConnections,
      enqueueMs: {
        p50: percentile(submitTimes, 0.5),
        p95: percentile(submitTimes, 0.95),
        p99: percentile(submitTimes, 0.99),
      },
      startDelayMs: starts.map((values, kind) => ({
        kind,
        p50: percentile(values, 0.5),
        p95: percentile(values, 0.95),
        p99: percentile(values, 0.99),
      })),
      application: {
        operations: appOperations,
        p95Ms: percentile(appTimes, 0.95),
        p99Ms: percentile(appTimes, 0.99),
      },
      eventLoopP99Ms: Math.round((loop.percentile(99) / 1e6) * 100) / 100,
      expiredAttempts: rows[0]!.expired,
      failedJobs: rows[0]!.failed,
      remainingExpiredHistory: rows[0]!.oldHistory,
      plans,
    };
  } finally {
    appDone = true;
    stop.abort();
    clearInterval(sampler);
    loop.disable();
    await Promise.all([...runners, ...applications]);
    await client.close();
    await database.queryClassic(
      'DELETE FROM pgclear_jobs.jobs WHERE name IN (SELECT jsonb_array_elements_text($1::jsonb))',
      [namesJson],
    );
  }
}

try {
  const { rows } = await database.queryClassic<{ version: string }>(
    "SELECT current_setting('server_version') AS version",
  );
  const profiles = [];
  for (const settings of [
    ['balanced', 4, 0, 2],
    ['contended', 2, 8, 12],
  ] as const) {
    process.stderr.write(
      `Running ${settings[0]} profile for ${durationMs} ms plus backlog drain.\n`,
    );
    profiles.push(await profile(settings[0], settings[1], settings[2], settings[3]));
  }
  const result = {
    recordedAt: new Date().toISOString(),
    node: process.version,
    postgres: rows[0]!.version,
    cpu: cpus()[0]?.model,
    availableCpus: availableParallelism(),
    durationMs,
    intervalMs,
    profiles,
  };
  const json = JSON.stringify(result, null, 2) + '\n';
  if (process.env.PGCLEAR_JOBS_LOAD_OUTPUT)
    await writeFile(process.env.PGCLEAR_JOBS_LOAD_OUTPUT, json);
  process.stdout.write(json);
} finally {
  await database.close();
}
