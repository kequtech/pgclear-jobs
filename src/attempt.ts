import { DriverError } from '@kequtech/pgclear';
import { errorText, JobError } from './errors.js';
import type { HandlerContext, JobDefinition } from './definition.js';
import { finishClaim, renewClaim } from './claims.js';
import type { Claim } from './claims.js';
import type { Execute } from './storage.js';
import type { JobsIntegration } from './integration.js';
import { pause, settleOrAbort } from './wakeup.js';

export async function executeAttempt<Database, Tx>(
  database: Database,
  integration: JobsIntegration<Database, Tx>,
  execute: Execute,
  claim: Claim,
  definition: JobDefinition<unknown, Tx>,
  leaseMs: number,
  abandon: AbortSignal,
  requestedAt: number,
  databaseMs: number,
): Promise<void> {
  // Fail closed if transport/pool delay spent the claim's budget. Monotonic time
  // avoids depending on synchronization between the database and worker clocks.
  const elapsed = performance.now() - requestedAt;
  const remainingLease = Math.min(leaseMs, claim.policy.executionTimeoutMs) - elapsed;
  if (remainingLease <= 0 || abandon.aborted) return;
  const controller = new AbortController();
  const monitors = new AbortController();
  const signal = AbortSignal.any([controller.signal, abandon]);
  let leaseTimer: ReturnType<typeof setTimeout> | undefined;
  const watchLease = (remaining: number) => {
    clearTimeout(leaseTimer);
    if (remaining <= 0) controller.abort(new Error('Job lease expired'));
    else leaseTimer = setTimeout(() => controller.abort(new Error('Job lease expired')), remaining);
  };
  watchLease(remainingLease);
  const context: HandlerContext = {
    id: claim.id,
    attempt: claim.attempt_count,
    signal,
    maxAttempts: claim.policy.maxAttempts,
    scheduledFor: claim.scheduled_for ? new Date(claim.scheduled_for) : null,
  };
  // The driver can roll back on cancellation before arbitrary user JavaScript settles.
  // Keep the original handler separately so cancellation never releases its local slot early.
  let userHandler: Promise<string | JobError | null> = Promise.resolve(null);
  const invoke = (work: () => void | Promise<void>) => {
    userHandler = Promise.resolve()
      .then(work)
      .then(
        () => null,
        (error: unknown) => (error instanceof JobError ? error : errorText(error)),
      );
    return userHandler;
  };
  const handler = Promise.resolve().then(async () => {
    if (signal.aborted) return;
    if (!definition.transactional) return invoke(() => definition.handler(claim.payload, context));
    // Only a confirmed handler failure is recorded outside the rolled-back transaction.
    // Transaction/commit failures propagate to supervision, including uncertain commits.
    const failure = new Error('Transactional handler failed');
    let outcome: string | JobError | null = null;
    try {
      await integration.transaction(
        database,
        async (tx) => {
          outcome = await invoke(() => definition.handler(claim.payload, { ...context, tx }));
          if (outcome !== null) {
            failure.cause = errorText(outcome);
            throw failure;
          }
          signal.throwIfAborted();
          const completionTimer = setTimeout(
            () => controller.abort(new DriverError('PGCLEAR_TIMEOUT', 'Job completion timed out.')),
            databaseMs,
          );
          try {
            if (!(await finishClaim(integration.execute(tx), claim, null))) {
              controller.abort(new Error('Job claim lost'));
              signal.throwIfAborted();
            }
          } finally {
            clearTimeout(completionTimer);
          }
        },
        { signal },
      );
      return null;
    } catch (error) {
      if (error === failure) return outcome;
      // A cancelled driver transaction is expected only after confirmed cleanup.
      // Never hide rollback/release failures (including AggregateError) or uncertain commits.
      if (signal.aborted && error instanceof DriverError && error.code === 'PGCLEAR_ABORTED')
        return;
      throw error;
    }
  });
  const heartbeat = async () => {
    let interval = Math.min(leaseMs / 3, remainingLease / 2);
    while (!monitors.signal.aborted) {
      await pause(Math.max(1, Math.floor(interval)), monitors.signal);
      if (monitors.signal.aborted) break;
      const renewingAt = performance.now();
      if (!(await renewClaim(execute, claim, leaseMs))) {
        controller.abort(new Error('Job claim lost'));
        break;
      }
      if (signal.aborted) break;
      const remaining = leaseMs - (performance.now() - renewingAt);
      watchLease(remaining);
      interval = Math.min(leaseMs / 3, remaining / 2);
    }
  };
  const beat = heartbeat();
  const timeout = setTimeout(
    () => controller.abort(new Error('Job execution deadline reached')),
    Math.max(1, claim.policy.executionTimeoutMs - elapsed),
  );
  try {
    const result = await Promise.race([settleOrAbort(handler, signal), beat]);
    if (!signal.aborted && result !== undefined && (!definition.transactional || result !== null)) {
      await finishClaim(execute, claim, result);
    }
  } finally {
    monitors.abort();
    controller.abort(new Error('Attempt finished'));
    clearTimeout(leaseTimer);
    clearTimeout(timeout);
    try {
      await beat;
    } finally {
      // Wait for transaction rollback/cleanup even if cancellation or renewal failure won.
      if (definition.transactional) await handler;
    }
  }
  // Cancellation ends claim ownership; the handler still occupies its local slot
  // until it settles or shutdown deliberately spends its grace.
  await settleOrAbort(userHandler, abandon);
}
