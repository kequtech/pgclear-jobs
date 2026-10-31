import assert from 'node:assert/strict';
import test from 'node:test';
import { Wakeup, settleOrAbort } from '../src/wakeup.js';

test('a notification before sleep is retained and repeated notifications coalesce', async () => {
  const wake = new Wakeup();
  const signal = new AbortController().signal;
  const before = wake.version;
  wake.notify();
  wake.notify();
  await wake.wait(before, 10_000, signal);
  let resumed = false;
  const next = wake.wait(wake.version, 10_000, signal).then(() => {
    resumed = true;
  });
  await Promise.resolve();
  assert.equal(resumed, false);
  wake.notify();
  await next;
});

test('notifications wake registered waiters, and abort cleans up an idle wait', async () => {
  const wake = new Wakeup();
  const stop = new AbortController();
  const first = wake.wait(wake.version, 10_000, stop.signal);
  const second = wake.wait(wake.version, 10_000, stop.signal);
  wake.notify();
  await Promise.all([first, second]);
  const idle = wake.wait(wake.version, 10_000, stop.signal);
  stop.abort();
  await idle;
  wake.notify();
});

test('waiting for a handler ends only when it settles or shutdown abandons it', async () => {
  const handler = Promise.withResolvers<void>();
  const stop = new AbortController();
  let finished = false;
  const waiting = settleOrAbort(handler.promise, stop.signal).then(() => {
    finished = true;
  });
  await Promise.resolve();
  assert.equal(finished, false);
  handler.resolve();
  await waiting;
  const uncooperative = new Promise<void>(() => {});
  const abandoned = settleOrAbort(uncooperative, stop.signal);
  stop.abort();
  await abandoned;
});
