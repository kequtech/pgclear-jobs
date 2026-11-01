import type { InputValue } from '@kequtech/pgclear';
import { executionPolicy, uuid } from './definition.js';
import type { ExecutionPolicy } from './definition.js';

export type Execute = <Row extends object>(
  text: string,
  values?: readonly InputValue[],
) => Promise<{ readonly rows: readonly Row[] }>;

export const states = ['pending', 'running', 'completed', 'failed', 'cancelled'] as const;
export type JobState = (typeof states)[number];

export interface Job<Payload = unknown> {
  readonly id: string;
  readonly name: string;
  readonly payload: Payload;
  readonly key: string | null;
  readonly state: JobState;
  readonly attempts: number;
  readonly policy: ExecutionPolicy;
  readonly runAt: Date;
  readonly createdAt: Date;
  readonly updatedAt: Date;
  readonly finishedAt: Date | null;
  readonly lastError: string | null;
  readonly scheduleId: string | null;
  readonly scheduledFor: Date | null;
  readonly retryOf: string | null;
  /** Opaque position for find({ after }); preserves PostgreSQL timestamp precision. */
  readonly cursor: string;
}

export function readCursor(cursor: string): readonly [string, string] {
  try {
    if (typeof cursor !== 'string' || cursor.length > 512) throw new TypeError();
    const value: unknown = JSON.parse(Buffer.from(cursor, 'base64url').toString('utf8'));
    if (
      Array.isArray(value) &&
      value.length === 2 &&
      typeof value[0] === 'string' &&
      /^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,6})?(?:Z|[+-]\d\d:\d\d)$/.test(value[0]) &&
      Number.isFinite(new Date(value[0]).getTime()) &&
      new Date(value[0].slice(0, 19) + 'Z').toISOString().slice(0, 19) === value[0].slice(0, 19) &&
      (value[0].endsWith('Z') || Number(value[0].slice(-5, -3)) <= 15)
    )
      return [value[0], uuid(value[1])];
  } catch {
    /* Report a public validation error before SQL. */
  }
  throw new TypeError('Invalid job cursor.');
}

export interface StoredJob {
  id: string;
  name: string;
  payload: unknown;
  key: string | null;
  state: JobState;
  attempt_count: number;
  policy: ExecutionPolicy;
  run_at: string;
  created_at: string;
  updated_at: string;
  finished_at: string | null;
  last_error: string | null;
  claim_token: string | null;
  deadline: string | null;
  schedule_id: string | null;
  scheduled_for: string | null;
  retry_of: string | null;
}

export function jobRecord<Payload = unknown>(row: StoredJob): Job<Payload> {
  return {
    id: row.id,
    name: row.name,
    payload: row.payload as Payload,
    key: row.key,
    state: row.state,
    attempts: row.attempt_count,
    policy: executionPolicy(row.policy),
    runAt: new Date(row.run_at),
    createdAt: new Date(row.created_at),
    updatedAt: new Date(row.updated_at),
    finishedAt: row.finished_at === null ? null : new Date(row.finished_at),
    lastError: row.last_error,
    scheduleId: row.schedule_id ?? null,
    scheduledFor: row.scheduled_for ? new Date(row.scheduled_for) : null,
    retryOf: row.retry_of ?? null,
    cursor: Buffer.from(JSON.stringify([row.created_at, row.id])).toString('base64url'),
  };
}
