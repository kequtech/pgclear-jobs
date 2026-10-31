import { setTimeout as delay } from 'node:timers/promises';
import { setMaxListeners } from 'node:events';
import { createPool, listen } from '@kequtech/pgclear';
import type { ConnectionOptions, Listener, Pool, PoolOptions } from '@kequtech/pgclear';
import { integer } from './definition.js';
import { errorText, JobError } from './errors.js';
import type { JobDefinition } from './definition.js';
import { claimJob, finishClaim, recoverClaims, renewClaim } from './claims.js';
import type { Claim } from './claims.js';
import { checkSchema, notificationChannel } from './schema.js';
import type { Execute } from './storage.js';
import { settleOrAbort, Wakeup } from './wakeup.js';
import { advanceSchedules } from './schedules.js';

export interface RunnerOptions {
  readonly signal?: AbortSignal;
  /** Maximum unfinished handlers in this runner, including timed-out handlers. Default 1. */
  readonly concurrency?: number;
  /** Listener connection for a borrowed pool, or false to disable notifications. */
  readonly notifications?: string | ConnectionOptions | false;
  /** Fallback interval: default 30000 with notifications, otherwise 1000. */
  readonly pollIntervalMs?: number;
  /** One recovery batch per interval, independently of execution slots. Default 1000. */
  readonly recoveryIntervalMs?: number;
  /** Heartbeats run every third of this interval. Default 30000. */
  readonly leaseMs?: number;
  /** Finish active work for this long after abort; then abandon claims. Default 30000. */
  readonly shutdownTimeoutMs?: number;
  /** Bound each driver operation, including pool acquisition and listener startup. Default 10000. */
  readonly databaseTimeoutMs?: number;
}

async function pause(ms: number, signal: AbortSignal): Promise<void> {
  try {
    await delay(ms, undefined, { signal });
  } catch (error) {
    if (!signal.aborted) throw error;
  }
}

async function executeClaim(
  execute: Execute,
  claim: Claim,
  definition: JobDefinition<unknown>,
  leaseMs: number,
  abandon: AbortSignal,
): Promise<void> {
  const controller = new AbortController();
  const monitors = new AbortController();
  const signal = AbortSignal.any([controller.signal, abandon]);
  const handler = Promise.resolve().then(async () => {
    if (signal.aborted) return;
    try {
      await definition.handler(claim.payload, {
        id: claim.id,
        attempt: claim.attempt_count,
        signal,
        maxAttempts: claim.policy.maxAttempts,
        scheduledFor: claim.scheduled_for ? new Date(claim.scheduled_for) : null,
      });
      return null;
    } catch (error) {
      return error instanceof JobError ? error : errorText(error);
    }
  });
  const heartbeat = async () => {
    while (!monitors.signal.aborted) {
      await pause(Math.max(1, Math.floor(leaseMs / 3)), monitors.signal);
      if (monitors.signal.aborted) break;
      if (!(await renewClaim(execute, claim, leaseMs))) {
        controller.abort(new Error('Job claim lost'));
        break;
      }
    }
  };
  const beat = heartbeat();
  const timeout = setTimeout(
    () => controller.abort(new Error('Job execution deadline reached')),
    claim.policy.executionTimeoutMs,
  );
  try {
    const result = await Promise.race([settleOrAbort(handler, signal), beat]);
    if (!signal.aborted && result !== undefined) {
      await finishClaim(execute, claim, result);
    }
  } finally {
    monitors.abort();
    controller.abort(new Error('Attempt finished'));
    clearTimeout(timeout);
    await beat;
  }
  // Cancellation ends claim ownership; the handler still occupies its local slot
  // until it settles or shutdown deliberately spends its grace.
  await settleOrAbort(handler, abandon);
}

function notificationWarning(error: unknown): void {
  process.emitWarning('Job notifications are unavailable; continuing with polling.', {
    code: 'TENON_JOBS_NOTIFICATIONS_UNAVAILABLE',
    detail: errorText(error),
  });
}

function listenerConnection(connection: string | PoolOptions): string | ConnectionOptions {
  if (typeof connection === 'string') return connection;
  const {
    maxConnections,
    maxWaiting,
    acquireTimeoutMs,
    idleTimeoutMs,
    maxLifetimeMs,
    ...settings
  } = connection;
  return settings;
}

/** A supplied pool is borrowed. Connection settings create a runner-owned pool and listener. */
export async function runJobs(
  connection: Pool | string | PoolOptions,
  definitions: readonly JobDefinition<unknown>[],
  options: RunnerOptions = {},
): Promise<void> {
  const concurrency = integer(options.concurrency ?? 1, 'concurrency', 1, 1000);
  const borrowed = typeof connection !== 'string' && 'queryClassic' in connection;
  const notifications =
    options.notifications ?? (borrowed ? false : listenerConnection(connection));
  const pollMs = integer(
    options.pollIntervalMs ?? (notifications === false ? 1000 : 30_000),
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
  const pool = borrowed ? connection : createPool(connection);
  const names = [...byName.keys()];
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
    if (notifications === false) return;
    while (!stop.signal.aborted) {
      let listener: Listener | undefined;
      const closed = Promise.withResolvers<void>();
      try {
        listener = await listen(listenerConnection(notifications), {
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
    listening = startListener().catch(fail);
    maintenance = (async () => {
      while (!stop.signal.aborted) {
        const recovered = await recoverClaims(execute);
        const scheduled = await advanceSchedules(pool, names, databaseMs);
        if (recovered || scheduled) wake.notify();
        await pause(recoveryMs, stop.signal);
      }
    })().catch(fail);
    while (!stop.signal.aborted) {
      const version = wake.version;
      const available = names.filter(
        (name) => (counts.get(name) ?? 0) < (byName.get(name)!.concurrency ?? concurrency),
      );
      if (active.size < concurrency && available.length) {
        const { claim, delayMs } = await claimJob(execute, available, leaseMs);
        if (stop.signal.aborted) break;
        if (claim) {
          counts.set(claim.name, (counts.get(claim.name) ?? 0) + 1);
          const task = executeClaim(
            execute,
            claim,
            byName.get(claim.name)!,
            leaseMs,
            abandon.signal,
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
        const backstop = notifications !== false && !connected ? Math.min(pollMs, 1000) : pollMs;
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
    if (!borrowed) await pool.close({ timeoutMs: databaseMs });
  }
  if (failures.length) throw failures[0];
}
