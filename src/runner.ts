import { setTimeout as delay } from 'node:timers/promises';
import type { Pool } from '@kequtech/pgclear';
import { integer } from './definition.js';
import type { JobDefinition } from './definition.js';
import { claimJob, finishClaim, recoverClaims, renewClaim } from './claims.js';
import type { Claim } from './claims.js';
import { checkSchema } from './storage.js';
import type { Execute } from './storage.js';

export interface RunnerOptions {
  readonly signal?: AbortSignal;
  /** Per process, default 1. */
  readonly concurrency?: number;
  /** Polling is authoritative, including delayed work. Default 1000. */
  readonly pollIntervalMs?: number;
  /** Heartbeats run every third of this interval. Default 30000. */
  readonly leaseMs?: number;
  /** Finish active work for this long after abort; then abandon claims. Default 30000. */
  readonly shutdownTimeoutMs?: number;
  /** Bound each driver operation, including pool acquisition. Default 10000. */
  readonly databaseTimeoutMs?: number;
}

async function pause(ms: number, signal: AbortSignal): Promise<void> {
  try {
    await delay(ms, undefined, { signal });
  } catch (error) {
    if (!signal.aborted) throw error;
  }
}

function errorText(error: unknown): string {
  // Store messages only, not stacks or payloads. Handlers must avoid secrets in messages.
  try {
    return (error instanceof Error ? `${error.name}: ${error.message}` : String(error)).slice(
      0,
      4000,
    );
  } catch {
    return 'Handler threw an unprintable error';
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
  const abandonAttempt = () => controller.abort(new Error('Runner shutdown deadline reached'));
  abandon.addEventListener('abort', abandonAttempt, { once: true });
  if (abandon.aborted) abandonAttempt();
  let onAbort: () => void = () => {};
  const interrupted = new Promise<{ kind: 'abandoned' }>((resolve) => {
    onAbort = () => resolve({ kind: 'abandoned' });
    controller.signal.addEventListener('abort', onAbort, { once: true });
    if (controller.signal.aborted) onAbort();
  });
  const heartbeat = async (): Promise<{ kind: 'abandoned' }> => {
    while (!monitors.signal.aborted) {
      await pause(Math.max(1, Math.floor(leaseMs / 3)), monitors.signal);
      if (monitors.signal.aborted) break;
      if (!(await renewClaim(execute, claim, leaseMs))) {
        controller.abort(new Error('Job claim lost'));
        break;
      }
    }
    return { kind: 'abandoned' };
  };
  const beat = heartbeat();
  try {
    const handler = Promise.resolve().then(async () => {
      if (controller.signal.aborted) return { kind: 'abandoned' as const };
      try {
        await definition.handler(claim.payload, {
          id: claim.id,
          attempt: claim.attempt_count,
          signal: controller.signal,
        });
        return { kind: 'finished' as const, error: null };
      } catch (error) {
        return { kind: 'finished' as const, error: errorText(error) };
      }
    });
    const timeout = pause(claim.policy.executionTimeoutMs, monitors.signal).then(() => {
      controller.abort(new Error('Job execution deadline reached'));
      return { kind: 'abandoned' as const };
    });
    const result = await Promise.race([handler, interrupted, timeout, beat]);
    if (result.kind === 'finished' && !controller.signal.aborted) {
      await finishClaim(execute, claim, result.error);
    }
    // Abandoned attempts retain their token until bounded recovery observes expiry.
  } finally {
    monitors.abort();
    controller.abort(new Error('Attempt finished'));
    controller.signal.removeEventListener('abort', onAbort);
    abandon.removeEventListener('abort', abandonAttempt);
    await beat;
  }
}

/** Long-running consumer. Rejects on database faults; the application owns restart policy. */
export async function runJobs(
  pool: Pool,
  definitions: readonly JobDefinition<unknown>[],
  options: RunnerOptions = {},
): Promise<void> {
  const concurrency = integer(options.concurrency ?? 1, 'concurrency', 1, 1000);
  const pollMs = integer(options.pollIntervalMs ?? 1000, 'pollIntervalMs');
  const leaseMs = integer(options.leaseMs ?? 30_000, 'leaseMs', 30);
  const shutdownMs = integer(options.shutdownTimeoutMs ?? 30_000, 'shutdownTimeoutMs', 0);
  const databaseMs = integer(options.databaseTimeoutMs ?? 10_000, 'databaseTimeoutMs');
  const byName = new Map(definitions.map((definition) => [definition.name, definition]));
  if (!byName.size || byName.size !== definitions.length) {
    throw new TypeError('Supply at least one definition, with no duplicate names.');
  }
  if (options.signal?.aborted) return;
  const stop = new AbortController();
  const abandon = new AbortController();
  let shutdownTimer: ReturnType<typeof setTimeout> | undefined;
  const shutdown = () => {
    stop.abort();
    if (shutdownTimer === undefined) shutdownTimer = setTimeout(() => abandon.abort(), shutdownMs);
  };
  options.signal?.addEventListener('abort', shutdown, { once: true });
  const execute: Execute = (text, values) =>
    pool.queryClassic(text, values, { timeoutMs: databaseMs });
  try {
    await checkSchema(execute);
    const worker = async () => {
      try {
        while (!stop.signal.aborted) {
          await recoverClaims(execute);
          if (stop.signal.aborted) break;
          const claim = await claimJob(execute, [...byName.keys()], leaseMs);
          if (claim) {
            // If shutdown arrived during the claim, deliberately abandon it for recovery.
            if (stop.signal.aborted) break;
            await executeClaim(execute, claim, byName.get(claim.name)!, leaseMs, abandon.signal);
          } else {
            await pause(pollMs, stop.signal);
          }
        }
      } catch (error) {
        stop.abort();
        abandon.abort();
        throw error;
      }
    };
    const results = await Promise.allSettled(Array.from({ length: concurrency }, worker));
    const failure = results.find((result) => result.status === 'rejected');
    if (failure?.status === 'rejected') throw failure.reason;
  } finally {
    options.signal?.removeEventListener('abort', shutdown);
    clearTimeout(shutdownTimer);
    abandon.abort();
  }
}
