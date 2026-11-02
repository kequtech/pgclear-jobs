import assert from 'node:assert/strict';
import { setImmediate as nextTurn } from 'node:timers/promises';
import test from 'node:test';
import { eventObserver } from '../src/observation.js';
import type { RunnerEvent } from '../src/observation.js';

const info = {
  id: 'one',
  name: 'example',
  attempt: 1,
  maxAttempts: 3,
  scheduledFor: new Date(1234),
};

test('event observers receive detached identity dates and cannot mutate queue metadata', () => {
  const retryAt = new Date(5678);
  const error = { original: true };
  const events: RunnerEvent[] = [];
  const observe = eventObserver((event) => {
    events.push(event);
  });
  observe({ ...info, type: 'attemptFailed', error, retryAt });
  observe({ type: 'runnerFailed', error, attempt: info });

  // Assert outside the isolated callback: observer exceptions deliberately cannot fail execution.
  assert.equal(events.length, 2);
  const [failed, runnerFailed] = events;
  assert.ok(failed?.type === 'attemptFailed');
  assert.equal(Object.isFrozen(failed), true);
  assert.equal(failed.error, error);
  assert.ok(failed.scheduledFor);
  assert.ok(failed.retryAt);
  assert.notEqual(failed.scheduledFor, info.scheduledFor);
  assert.notEqual(failed.retryAt, retryAt);
  assert.equal(failed.scheduledFor.getTime(), 1234);
  assert.equal(failed.retryAt.getTime(), 5678);
  failed.scheduledFor.setTime(0);
  failed.retryAt.setTime(0);

  assert.ok(runnerFailed?.type === 'runnerFailed');
  assert.equal(Object.isFrozen(runnerFailed), true);
  assert.equal(runnerFailed.error, error);
  assert.ok(runnerFailed.attempt?.scheduledFor);
  assert.equal(Object.isFrozen(runnerFailed.attempt), true);
  assert.notEqual(runnerFailed.attempt, info);
  assert.notEqual(runnerFailed.attempt.scheduledFor, info.scheduledFor);
  assert.equal(runnerFailed.attempt.scheduledFor.getTime(), 1234);
  runnerFailed.attempt.scheduledFor.setTime(0);

  assert.equal(info.scheduledFor.getTime(), 1234);
  assert.equal(retryAt.getTime(), 5678);
});

test('sync, async and thenable observer errors warn without escaping observation', async (t) => {
  const warnings: unknown[] = [];
  t.mock.method(process, 'emitWarning', (...args: unknown[]) => {
    warnings.push(args);
  });
  const error = new Error('logger failed');
  eventObserver(() => {
    throw error;
  })({ ...info, type: 'attemptStarted' });
  eventObserver(async () => {
    throw error;
  })({ ...info, type: 'attemptCompleted' });
  const broken = Promise.resolve();
  Object.defineProperty(broken, 'then', {
    get() {
      throw error;
    },
  });
  eventObserver(() => broken)({ ...info, type: 'attemptStarted' });
  await nextTurn();
  assert.equal(warnings.length, 3);
  for (const warning of warnings)
    assert.equal((warning as [unknown, { code: string }])[1].code, 'PGCLEAR_JOBS_OBSERVER_FAILED');
});
