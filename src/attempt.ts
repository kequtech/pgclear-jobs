import { DriverError } from '@kequtech/pgclear';
import { errorText, JobError } from './errors.js';
import type { HandlerContext, JobDefinition } from './definition.js';
import { finishAttempt, finishCancellation, renewClaim } from './claims.js';
import type { AttemptOutcome, Claim } from './claims.js';
import type { Execute } from './storage.js';
import type { JobsIntegration } from './integration.js';
import { attemptInfo } from './observation.js';
import type { AttemptAbandonReason, RunnerEvent, RunnerHooks } from './observation.js';
import { pause, settleOrAbort } from './wakeup.js';

type HandlerOutcome = { readonly ok: true } | { readonly ok: false; readonly error: unknown };

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
  emit: (event: RunnerEvent) => void,
  onHandlerError: RunnerHooks['onHandlerError'],
): Promise<void> {
  const info = attemptInfo(claim);
  const elapsed = performance.now() - requestedAt;
  const remainingLease = Math.min(leaseMs, claim.policy.executionTimeoutMs) - elapsed;
  const initialExpiry: AttemptAbandonReason =
    claim.policy.executionTimeoutMs <= leaseMs ? 'deadlineExceeded' : 'leaseLost';
  if (remainingLease <= 0 || abandon.aborted) {
    emit({
      ...info,
      type: 'attemptAbandoned',
      reason: abandon.aborted ? 'runnerStopped' : initialExpiry,
    });
    return;
  }
  const controller = new AbortController();
  const monitors = new AbortController();
  const signal = AbortSignal.any([controller.signal, abandon]);
  let abandoned: AttemptAbandonReason | undefined;
  const lose = (reason: AttemptAbandonReason, error: unknown) => {
    if (signal.aborted) return;
    abandoned = reason;
    controller.abort(error);
  };
  let leaseTimer: ReturnType<typeof setTimeout> | undefined;
  const watchLease = (remaining: number, reason: AttemptAbandonReason = 'leaseLost') => {
    clearTimeout(leaseTimer);
    const expired = () =>
      lose(
        reason,
        new Error(
          reason === 'deadlineExceeded' ? 'Job execution deadline reached' : 'Job lease expired',
        ),
      );
    if (remaining <= 0) expired();
    else leaseTimer = setTimeout(expired, remaining);
  };
  watchLease(remainingLease, initialExpiry);
  const context: HandlerContext = {
    id: info.id,
    attempt: info.attempt,
    maxAttempts: info.maxAttempts,
    scheduledFor: info.scheduledFor ? new Date(info.scheduledFor) : null,
    signal,
  };
  // Driver rollback and application JavaScript can settle independently.
  let userHandler: Promise<HandlerOutcome> = Promise.resolve({ ok: true });
  const invoke = (work: () => void | Promise<void>) => {
    emit({ ...info, type: 'attemptStarted' });
    userHandler = Promise.resolve()
      .then(work)
      .then<HandlerOutcome, HandlerOutcome>(
        () => ({ ok: true }),
        (error: unknown) => ({ ok: false, error }),
      );
    return userHandler;
  };
  let completionRefused = false;
  const completion: { committed: AttemptOutcome | null } = { committed: null };
  const handler = Promise.resolve().then(async (): Promise<HandlerOutcome | undefined> => {
    if (signal.aborted) return;
    if (!definition.transactional) return invoke(() => definition.handler(claim.payload, context));
    const failure = new Error('Transactional handler failed');
    let outcome: HandlerOutcome = { ok: true };
    try {
      completion.committed = await integration.transaction(
        database,
        async (tx) => {
          outcome = await invoke(() => definition.handler(claim.payload, { ...context, tx }));
          if (!outcome.ok) {
            failure.cause = outcome.error;
            throw failure;
          }
          signal.throwIfAborted();
          const timer = setTimeout(
            () => controller.abort(new DriverError('PGCLEAR_TIMEOUT', 'Job completion timed out.')),
            databaseMs,
          );
          try {
            const finished = await finishAttempt(integration.execute(tx), claim, null);
            if (!finished) {
              completionRefused = true;
              lose('leaseLost', new Error('Job claim lost'));
              signal.throwIfAborted();
            }
            return finished;
          } finally {
            clearTimeout(timer);
          }
        },
        { signal },
      );
      return { ok: true };
    } catch (error) {
      if (error === failure) return outcome; // Managed rollback and cleanup are confirmed.
      if (signal.aborted && error instanceof DriverError && error.code === 'PGCLEAR_ABORTED')
        return;
      throw error; // Includes unknown commits and cleanup aggregates.
    }
  });
  const heartbeat = async () => {
    let interval = Math.min(leaseMs / 3, remainingLease / 2);
    while (!monitors.signal.aborted) {
      await pause(Math.max(1, Math.floor(interval)), monitors.signal);
      if (monitors.signal.aborted) break;
      const renewingAt = performance.now();
      const renewal = await renewClaim(execute, claim, leaseMs);
      if (renewal === 'cancelled') {
        lose('cancelRequested', new Error('Job cancellation requested'));
        break;
      }
      if (!renewal) {
        lose('leaseLost', new Error('Job claim lost'));
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
    () => lose('deadlineExceeded', new Error('Job execution deadline reached')),
    Math.max(1, claim.policy.executionTimeoutMs - elapsed),
  );
  let outcomeEvent: RunnerEvent | undefined;
  const failures: unknown[] = [];
  const remember = (error: unknown) => {
    if (!failures.includes(error)) failures.push(error);
  };
  try {
    const result = await Promise.race([settleOrAbort(handler, signal), beat]);
    if (!completion.committed && !signal.aborted && result !== undefined) {
      if (!result.ok && onHandlerError) {
        const decision = Promise.resolve().then(() =>
          onHandlerError(
            Object.freeze({
              ...attemptInfo(claim),
              error: result.error,
              signal,
              transactional: definition.transactional === true,
            }),
          ),
        );
        let action;
        try {
          action = await settleOrAbort(decision, signal);
        } catch (error) {
          throw new AggregateError([result.error, error], 'The handler error policy failed.');
        }
        if (!signal.aborted) {
          if (action === 'stop') throw result.error;
          if (action !== 'continue')
            throw new AggregateError(
              [result.error, new TypeError('onHandlerError must return continue or stop.')],
              'The handler error policy returned an invalid action.',
            );
        }
      }
      if (!signal.aborted) {
        const diagnostic = result.ok
          ? null
          : result.error instanceof JobError
            ? result.error
            : errorText(result.error);
        const finished = await finishAttempt(execute, claim, diagnostic);
        if (finished) {
          outcomeEvent = result.ok
            ? { ...info, type: 'attemptCompleted' }
            : {
                ...info,
                type: 'attemptFailed',
                error: result.error,
                retryAt: finished.state === 'pending' ? finished.runAt : null,
              };
        } else {
          completionRefused = true;
          lose('leaseLost', new Error('Job claim lost'));
        }
      }
    }
  } catch (error) {
    remember(error);
  } finally {
    // Save the cancellation reason before our internal cleanup abort.
    abandoned ??= abandon.aborted ? 'runnerStopped' : undefined;
    monitors.abort();
    controller.abort(new Error('Attempt finished'));
    clearTimeout(leaseTimer);
    clearTimeout(timeout);
    try {
      await beat;
    } catch (error) {
      remember(error);
    }
    if (definition.transactional) {
      try {
        await handler;
      } catch (error) {
        remember(error);
      }
    }
  }
  // A commit can settle after cancellation won the race. Observe it only once acknowledged.
  if (completion.committed) outcomeEvent = { ...info, type: 'attemptCompleted' };
  const finishingCancellation =
    !outcomeEvent &&
    !failures.length &&
    !abandon.aborted &&
    (abandoned === 'cancelRequested' || completionRefused);
  if (finishingCancellation) {
    await settleOrAbort(userHandler, abandon);
    if (!abandon.aborted && (await finishCancellation(execute, claim)))
      outcomeEvent = { ...info, type: 'attemptCancelled' };
  }
  if (outcomeEvent) emit(outcomeEvent);
  if (failures.length === 1) throw failures[0];
  if (failures.length > 1) throw new AggregateError(failures, 'Job attempt and cleanup failed.');
  if (!outcomeEvent && abandoned) emit({ ...info, type: 'attemptAbandoned', reason: abandoned });
  if (!finishingCancellation) await settleOrAbort(userHandler, abandon);
}
