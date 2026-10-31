import assert from 'node:assert/strict';
import test from 'node:test';
import { JobError, previewSchedule } from '../src/index.js';

test('schedule previews are strictly after from and respect the selected time zone', () => {
  const from = new Date('2026-01-01T00:00:00Z');
  assert.deepEqual(
    previewSchedule({ cron: '0 3 * * *' }, { from, count: 2 }).map((d) => d.toISOString()),
    ['2026-01-01T03:00:00.000Z', '2026-01-02T03:00:00.000Z'],
  );
  assert.equal(
    previewSchedule(
      { cron: '0 3 * * *', timeZone: 'Europe/Budapest' },
      { from, count: 1 },
    )[0]!.toISOString(),
    '2026-01-01T02:00:00.000Z',
  );
  assert.equal(
    previewSchedule(
      { cron: '0 3 * * *' },
      { from: new Date('2026-01-01T03:00:00Z'), count: 1 },
    )[0]!.toISOString(),
    '2026-01-02T03:00:00.000Z',
  );
});

test('calendar schedules follow daylight saving changes and reject invalid configuration', () => {
  const dates = previewSchedule(
    { cron: '0 9 * * *', timeZone: 'Europe/Budapest' },
    { from: new Date('2026-03-28T00:00:00Z'), count: 2 },
  );
  assert.deepEqual(
    dates.map((d) => d.toISOString()),
    ['2026-03-28T08:00:00.000Z', '2026-03-29T07:00:00.000Z'],
  );
  for (const options of [
    { cron: '* * * * * *' },
    { cron: 'H * * * *' },
    { cron: '61 * * * *' },
    { cron: '0 3 * * *', timeZone: 'Somewhere/Invalid' },
  ]) {
    assert.throws(() => previewSchedule(options));
  }
  assert.throws(() => previewSchedule({ cron: '* * * * *' }, { count: 0 }));
});

test('retry refinements validate their options and snapshot the retry instant', () => {
  const at = new Date('2026-12-01T00:00:00Z');
  const error = new JobError('rate limited', { retryAt: at });
  at.setFullYear(2030);
  assert.equal(error.retryAt, '2026-12-01T00:00:00.000Z');
  assert.equal(new JobError('invalid', { retry: false }).retry, false);
  assert.throws(() => new JobError('invalid', { retryAt: new Date(NaN) }));
  assert.throws(() => new JobError('invalid', { retry: false, retryAt: new Date() }));
});

test('calendar previews handle a missing local time and choose one occurrence in a repeated hour', () => {
  const options = { cron: '30 2 * * *', timeZone: 'Europe/Budapest' };
  assert.deepEqual(
    previewSchedule(options, { from: new Date('2026-03-28T00:00:00Z'), count: 3 }).map((d) =>
      d.toISOString(),
    ),
    ['2026-03-28T01:30:00.000Z', '2026-03-29T01:30:00.000Z', '2026-03-30T00:30:00.000Z'],
  );
  assert.deepEqual(
    previewSchedule(options, { from: new Date('2026-10-24T00:00:00Z'), count: 3 }).map((d) =>
      d.toISOString(),
    ),
    ['2026-10-24T00:30:00.000Z', '2026-10-25T00:30:00.000Z', '2026-10-26T01:30:00.000Z'],
  );
});
