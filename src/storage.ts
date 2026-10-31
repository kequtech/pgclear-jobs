import type { InputValue } from '@kequtech/pgclear';
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
}

export function jobRecord<Payload = unknown>(row: StoredJob): Job<Payload> {
  return {
    id: row.id,
    name: row.name,
    payload: row.payload as Payload,
    key: row.key,
    state: row.state,
    attempts: row.attempt_count,
    policy: row.policy,
    runAt: new Date(row.run_at),
    createdAt: new Date(row.created_at),
    updatedAt: new Date(row.updated_at),
    finishedAt: row.finished_at === null ? null : new Date(row.finished_at),
    lastError: row.last_error,
  };
}
