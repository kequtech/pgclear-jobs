import assert from 'node:assert/strict';
import test from 'node:test';
import { defineJob } from '../src/index.js';
import { serializePayload } from '../src/definition.js';

test('definitions are inert, immutable and preserve the handler', () => {
  let called = false;
  const handler = (_payload: { value: number }) => {
    called = true;
  };
  const definition = defineJob({ name: 'example', handler });
  assert.equal(called, false);
  assert.equal(definition.handler, handler);
  assert.equal(definition.maxAttempts, 3);
  assert.equal(Object.isFrozen(definition), true);
});

test('invalid policies and lossy JSON payloads fail before database access', () => {
  for (const options of [
    { maxAttempts: 0 },
    { retryDelayMs: -1 },
    { retryMaxDelayMs: -1 },
    { concurrency: 0 },
    { executionTimeoutMs: Infinity },
  ]) {
    assert.throws(() => defineJob({ name: 'example', handler() {}, ...options }), TypeError);
  }
  assert.throws(() => defineJob({ name: ' ', handler() {} }), TypeError);
  const circular: { self?: unknown } = {};
  circular.self = circular;
  for (const value of [
    undefined,
    { x: undefined },
    NaN,
    1n,
    new Date(),
    [, 1],
    circular,
    '\0',
    '\ud800',
    { ['\udfff']: 1 },
  ]) {
    assert.throws(() => serializePayload(value), TypeError);
  }
  assert.deepEqual(JSON.parse(serializePayload({ data: [1, true, null, 'ok 😀'] })), {
    data: [1, true, null, 'ok 😀'],
  });
});
