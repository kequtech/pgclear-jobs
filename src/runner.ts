import { setMaxListeners } from 'node:events';
import { listen } from '@kequtech/pgclear';
import type { ConnectionOptions, Listener, Pool } from '@kequtech/pgclear';
import { integer, validateOptions } from './validation.js';
import { errorText } from './errors.js';
import type { JobDefinition } from './definition.js';
import { claimJob, recoverClaims } from './claims.js';
import { checkSchema, notificationChannel } from './schema.js';
import type { Execute } from './storage.js';
import { pause, settleOrAbort, Wakeup } from './wakeup.js';
import { advanceSchedules } from './schedules.js';
import { pruneCompleted } from './retention.js';
import type { JobsIntegration } from './integration.js';
import { pgclearIntegration } from './pgclear.js';
import { executeAttempt } from './attempt.js';

export interface RunnerOptions {
  readonly signal?: AbortSignal;
  /** Called once after storage validation, before dispatch. Awaited; rejection fails startup. */
  readonly onReady?: () => void | Promise<void>;
  /** Maximum unfinished handlers in this runner, including timed-out handlers. Default 1. */
  readonly concurrency?: number;
  /** Optional dedicated listener connection. Omit to use polling. */
  readonly notifications?: string | ConnectionOptions;
  /** Fallback interval: default 30000 with notifications, otherwise 1000. */
  readonly pollIntervalMs?: number;
  /** One recovery batch per interval, independently of execution slots. Default 1000. */
  readonly recoveryIntervalMs?: number;
  /** Heartbeats run every third of this interval. Default 30000. */
  readonly leaseMs?: number;
  /** Finish active work for this long after abort; then abandon claims. Default 30000. */
  readonly shutdownTimeoutMs?: number;
  /** Bound queue statements, maintenance and listener startup. Default 10000. */
  readonly databaseTimeoutMs?: number;
  /** Completed/cancelled history: default 7 days. False disables automatic cleanup. */
  readonly retentionMs?: number | false;
}

function notificationWarning(error: unknown): void {
  process.emitWarning('Job notifications are unavailable; continuing with polling.', {
    code: 'PGCLEAR_JOBS_NOTIFICATIONS_UNAVAILABLE',
    detail: errorText(error),
  });
}

/** Run committed work using an application-owned pool; the caller observes and supervises this promise. */
export async function runJobs(
  pool: Pool,
  definitions: readonly JobDefinition<unknown>[],
  options: RunnerOptions = {},
): Promise<void> {
  return runJobsWithIntegration(pool, definitions, options, pgclearIntegration);
}

/** Shared engine; integrations own the application transaction API, never queue policy. */
export async function runJobsWithIntegration<Database, Tx>(
  database: Database,
  definitions: readonly JobDefinition<unknown, Tx>[],
  options: RunnerOptions,
  integration: JobsIntegration<Database, Tx>,
): Promise<void> {
  validateOptions(options, [
    'signal',
    'onReady',
    'concurrency',
    'notifications',
    'pollIntervalMs',
    'recoveryIntervalMs',
    'leaseMs',
    'shutdownTimeoutMs',
    'databaseTimeoutMs',
    'retentionMs',
  ]);
  const onReady = options.onReady;
  if (onReady !== undefined && typeof onReady !== 'function')
    throw new TypeError('onReady must be a function.');
  const concurrency = integer(options.concurrency ?? 1, 'concurrency', 1, 1000);
  const notifications = options.notifications;
  if (
    notifications !== undefined &&
    typeof notifications !== 'string' &&
    (typeof notifications !== 'object' || notifications === null)
  )
    throw new TypeError('notifications must contain listener connection settings.');
  const retentionMs =
    options.retentionMs === false
      ? false
      : integer(options.retentionMs ?? 7 * 86_400_000, 'retentionMs', 1, Number.MAX_SAFE_INTEGER);
  const pollMs = integer(
    options.pollIntervalMs ?? (notifications === undefined ? 1000 : 30_000),
    'pollIntervalMs',
  );
  const recoveryMs = integer(options.recoveryIntervalMs ?? 1000, 'recoveryIntervalMs');
  const leaseMs = integer(options.leaseMs ?? 30_000, 'leaseMs', 30);
  const shutdownMs = integer(options.shutdownTimeoutMs ?? 30_000, 'shutdownTimeoutMs', 0);
  const databaseMs = integer(options.databaseTimeoutMs ?? 10_000, 'databaseTimeoutMs');
  const byName = new Map(definitions.map((definition) => [definition.name, definition]));
  if (!byName.size || byName.size !== definitions.length) {
    throw new TypeError('Supply at least one definition, with no duplicate names.');
  }
  if (options.signal?.aborted) return;
  const pool = integration.pool(database);
  if (!pool || typeof pool.queryClassic !== 'function' || typeof pool.transaction !== 'function')
    throw new TypeError('runJobs requires an application-owned pgclear pool.');
  const names = [...byName.keys()];
  let nextDefinition = 0;
  const stop = new AbortController();
  const abandon = new AbortController();
  setMaxListeners(concurrency + 1, abandon.signal);
  const wake = new Wakeup();
  const active = new Set<Promise<void>>();
  const counts = new Map<string, number>();
  const failures: unknown[] = [];
  let connected = false;
  let shutdownTimer: ReturnType<typeof setTimeout> | undefined;
  const shutdown = () => {
    stop.abort();
    if (shutdownTimer === undefined) shutdownTimer = setTimeout(() => abandon.abort(), shutdownMs);
  };
  const fail = (error: unknown) => {
    if (!failures.length) failures.push(error);
    stop.abort();
    abandon.abort();
  };
  options.signal?.addEventListener('abort', shutdown, { once: true });
  const execute: Execute = (text, values) =>
    pool.queryClassic(text, values, { timeoutMs: databaseMs });
  let maintenance: Promise<void> | undefined;
  let listening: Promise<void> | undefined;
  const startListener = async () => {
    if (notifications === undefined) return;
    while (!stop.signal.aborted) {
      let listener: Listener | undefined;
      const closed = Promise.withResolvers<void>();
      try {
        listener = await listen(notifications, {
          channels: [notificationChannel],
          reconnect: true,
          signal: stop.signal,
          timeoutMs: databaseMs,
          onNotification: ({ payload }) => {
            if (byName.has(payload)) wake.notify();
          },
          onStatus: (status) => {
            connected = status.state === 'connected';
            if (status.state === 'disconnected') notificationWarning(status.error);
            if (status.state === 'closed') closed.resolve();
            // Always reconcile after registration or a delivery gap.
            wake.notify();
          },
        });
        await settleOrAbort(closed.promise, stop.signal);
      } catch (error) {
        if (!stop.signal.aborted) notificationWarning(error);
      } finally {
        connected = false;
        await listener?.close({ timeoutMs: databaseMs });
      }
      if (!stop.signal.aborted) await pause(1000, stop.signal);
    }
  };
  try {
    await checkSchema(execute);
    if (stop.signal.aborted) return;
    if (onReady) {
      const ready = Promise.resolve(onReady());
      // The callback can request shutdown before returning its promise.
      if (stop.signal.aborted) void ready.catch(() => {});
      else await settleOrAbort(ready, stop.signal);
    }
    if (stop.signal.aborted) return;
    listening = startListener().catch(fail);
    maintenance = (async () => {
      let nextCleanup = 0;
      while (!stop.signal.aborted) {
        const recovered = await recoverClaims(execute);
        const scheduled = await advanceSchedules(pool, names, databaseMs);
        if (recovered || scheduled) wake.notify();
        if (retentionMs !== false && performance.now() >= nextCleanup) {
          const removed = await pruneCompleted(execute, names, retentionMs);
          // Drain a large expired history in bounded passes; otherwise check once a minute.
          nextCleanup = removed === 100 ? 0 : performance.now() + 60_000;
        }
        await pause(recoveryMs, stop.signal);
      }
    })().catch(fail);
    while (!stop.signal.aborted) {
      const version = wake.version;
      const available = [...names.slice(nextDefinition), ...names.slice(0, nextDefinition)].filter(
        (name) => (counts.get(name) ?? 0) < (byName.get(name)!.concurrency ?? concurrency),
      );
      if (active.size < concurrency && available.length) {
        const requestedAt = performance.now();
        const { claim, delayMs } = await claimJob(execute, available, leaseMs);
        if (stop.signal.aborted) break;
        if (claim) {
          // Rotate successful claims so an older backlog cannot monopolize every local slot.
          nextDefinition = (names.indexOf(claim.name) + 1) % names.length;
          counts.set(claim.name, (counts.get(claim.name) ?? 0) + 1);
          const task = executeAttempt(
            database,
            integration,
            execute,
            claim,
            byName.get(claim.name)!,
            leaseMs,
            abandon.signal,
            requestedAt,
            databaseMs,
          )
            .catch(fail)
            .finally(() => {
              active.delete(task);
              counts.set(claim.name, counts.get(claim.name)! - 1);
              wake.notify();
            });
          active.add(task);
          continue;
        }
        const backstop =
          notifications !== undefined && !connected ? Math.min(pollMs, 1000) : pollMs;
        await wake.wait(version, Math.min(backstop, Math.max(1, delayMs ?? backstop)), stop.signal);
      } else {
        await wake.wait(version, 2_147_483_647, stop.signal);
      }
    }
  } catch (error) {
    fail(error);
  } finally {
    stop.abort();
    await Promise.all([...active, maintenance, listening]);
    options.signal?.removeEventListener('abort', shutdown);
    clearTimeout(shutdownTimer);
  }
  if (failures.length) throw failures[0];
}
