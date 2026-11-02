import type { Claim } from './claims.js';
import type { HandlerContext } from './definition.js';
import { errorText } from './errors.js';

/** Attempt identity only; events and error policies never receive the job payload. */
export interface AttemptInfo {
  readonly id: string;
  readonly name: string;
  readonly attempt: number;
  readonly maxAttempts: number;
  readonly scheduledFor: Date | null;
}

export interface HandlerFailure extends AttemptInfo {
  readonly error: unknown;
  readonly signal: HandlerContext['signal'];
  readonly transactional: boolean;
}

export type HandlerErrorAction = 'continue' | 'stop';
export type AttemptAbandonReason =
  'leaseLost' | 'deadlineExceeded' | 'runnerStopped' | 'cancelRequested';

/** Local best-effort observations, not a durable or deployment-wide event stream. */
export type RunnerEvent =
  | (AttemptInfo & { readonly type: 'attemptStarted' })
  | (AttemptInfo & { readonly type: 'attemptCompleted' })
  | (AttemptInfo & { readonly type: 'attemptCancelled' })
  | (AttemptInfo & {
      readonly type: 'attemptFailed';
      readonly error: unknown;
      /** Confirmed retry time; null means the job became terminally failed. */
      readonly retryAt: Date | null;
    })
  | (AttemptInfo & {
      readonly type: 'attemptAbandoned';
      readonly reason: AttemptAbandonReason;
    })
  | {
      readonly type: 'runnerFailed';
      readonly error: unknown;
      readonly attempt?: AttemptInfo;
    };

export interface RunnerHooks {
  /** Observational only. Not awaited; failures warn and cannot change queue outcomes. */
  readonly onEvent?: (event: RunnerEvent) => void | Promise<void>;
  /** Called before failure bookkeeping, after managed rollback. Defaults to continue. */
  readonly onHandlerError?: (
    failure: HandlerFailure,
  ) => HandlerErrorAction | Promise<HandlerErrorAction>;
}

/** Build detached identity metadata without sharing mutable Dates with a handler. */
export function attemptInfo(claim: Claim): AttemptInfo {
  return {
    id: claim.id,
    name: claim.name,
    attempt: claim.attempt_count,
    maxAttempts: claim.policy.maxAttempts,
    scheduledFor: claim.scheduled_for ? new Date(claim.scheduled_for) : null,
  };
}

function snapshot(info: AttemptInfo): AttemptInfo {
  return Object.freeze({
    ...info,
    scheduledFor: info.scheduledFor ? new Date(info.scheduledFor) : null,
  });
}

/** Isolate both synchronous and asynchronous observer failures from queue execution. */
export function eventObserver(callback: RunnerHooks['onEvent']): (event: RunnerEvent) => void {
  const warn = (error: unknown) => {
    process.emitWarning('A Jobs event observer failed.', {
      code: 'PGCLEAR_JOBS_OBSERVER_FAILED',
      detail: errorText(error),
    });
  };
  return (event) => {
    if (!callback) return;
    try {
      const observation =
        event.type === 'runnerFailed'
          ? Object.freeze({
              ...event,
              ...(event.attempt ? { attempt: snapshot(event.attempt) } : {}),
            })
          : Object.freeze({
              ...event,
              scheduledFor: event.scheduledFor ? new Date(event.scheduledFor) : null,
              ...(event.type === 'attemptFailed'
                ? { retryAt: event.retryAt ? new Date(event.retryAt) : null }
                : {}),
            });
      void Promise.resolve(callback(observation)).catch(warn);
    } catch (error) {
      warn(error);
    }
  };
}
