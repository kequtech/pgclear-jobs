import assert from 'node:assert/strict';
import test from 'node:test';
import { defineJob, defineWorker } from '../src/index.js';
import { serializePayload } from '../src/validation.js';

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
  assert.equal(defineJob({ name: 'plain', handler() {} }).transactional, undefined);
  assert.equal(
    defineJob({ name: 'transactional', transactional: true, handler() {} }).transactional,
    true,
  );
  assert.throws(() => {
    // @ts-expect-error Runtime options also reject invalid transaction flags.
    defineJob({ name: 'invalid', transactional: 'yes', handler() {} });
  }, TypeError);
  assert.throws(() => {
    // @ts-expect-error An invalid handler must fail before execution.
    defineJob({ name: 'invalid', transactional: true, handler: null });
  }, TypeError);
});

test('misspelled definition options fail before silently changing execution policy', () => {
  for (const options of [{ maxAttempt: 1 }, { executionTimoutMs: 1000 }, { transaction: true }]) {
    assert.throws(() => defineJob({ name: 'example', handler() {}, ...options }), TypeError);
  }
});

test('producer specifications are inert and bind a worker without changing submission policy', () => {
  const spec = defineJob<{ id: string }>({ name: 'separated', maxAttempts: 7 });
  assert.equal(Object.isFrozen(spec), true);
  assert.equal('handler' in spec, false);
  const worker = defineWorker(spec, {
    concurrency: 2,
    handler(payload) {
      void payload.id;
    },
  });
  assert.equal(Object.isFrozen(worker), true);
  assert.equal(worker.name, spec.name);
  assert.equal(worker.maxAttempts, 7);
  assert.equal(worker.concurrency, 2);
  assert.equal('concurrency' in spec, false);
  assert.throws(() => defineWorker(spec, { handler: undefined } as never), TypeError);
  assert.throws(() => defineWorker(spec, {} as never), TypeError);
  assert.throws(() => defineWorker(spec, { handler() {}, maxAttempts: 1 } as never), TypeError);
  assert.throws(() => defineJob({ name: 'invalid', concurrency: 1 } as never), TypeError);
});
