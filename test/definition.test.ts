import assert from 'node:assert/strict';
import test from 'node:test';
import { defineJob } from '../src/index.js';
import { serializePayload } from '../src/definition.js';
import { defineJob as defineTenonJob } from '../src/tenon.js';

test('definitions are inert, immutable and preserve the handler', () => {
  let called = false;
  const handler = (_payload: { value: number }) => {
    called = true;
  };
  const definition = defineJob({ name: 'example', handler });
  assert.equal(called, false);
  assert.equal(definition.handler, handler);
  assert.equal(definition.maxAttempts, 3);
  assert.equal(definition.retryBackoff, 'exponential');
  assert.equal(definition.retryJitter, true);
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

test('optional object fields serialize naturally without changing array positions', () => {
  const payload = { id: 'one', optional: undefined, nested: { absent: undefined, value: 1 } };
  assert.deepEqual(JSON.parse(serializePayload(payload)), { id: 'one', nested: { value: 1 } });
  assert.equal(Object.hasOwn(payload, 'optional'), true);
  assert.throws(() => serializePayload([undefined]), TypeError);
  assert.throws(() => serializePayload(undefined), TypeError);
});

test('transactional execution is opt-in and validates the handler and flag', () => {
  assert.equal(
    defineJob({ name: 'transactional', transactional: true, handler() {} }).transactional,
    true,
  );
  assert.equal(
    defineTenonJob({ name: 'transactional', transactional: true, handler() {} }).transactional,
    true,
  );
  for (const define of [defineJob, defineTenonJob]) {
    assert.equal(define({ name: 'plain', handler() {} }).transactional, undefined);
    // @ts-expect-error Runtime options also reject invalid transaction flags.
    assert.throws(() => define({ name: 'invalid', transactional: 'yes', handler() {} }), TypeError);
    // @ts-expect-error Adapters must not hide an invalid original handler.
    assert.throws(() => define({ name: 'invalid', transactional: true, handler: null }), TypeError);
  }
});
