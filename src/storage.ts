import type { InputValue, Pool } from '@kequtech/pgclear';
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

/** Explicit, transactional installation. Never called by producers or runners. */
export async function installJobs(pool: Pool): Promise<void> {
  await pool.transaction(async (tx) => {
    await tx.queryClassic('SELECT pg_advisory_xact_lock(187032712, 1)');
    await tx.queryClassic('CREATE SCHEMA IF NOT EXISTS tenon_jobs');
    await tx.queryClassic(`CREATE TABLE IF NOT EXISTS tenon_jobs.migrations (
      version integer PRIMARY KEY, installed_at timestamptz NOT NULL DEFAULT clock_timestamp()
    )`);
    const { rows } = await tx.queryClassic<{ version: number }>(
      'SELECT version FROM tenon_jobs.migrations ORDER BY version DESC',
    );
    if (rows[0] && rows[0].version !== 1) throw new Error('Unsupported Tenon Jobs schema version.');
    if (rows.length) return;
    await tx.queryClassic(`CREATE TABLE tenon_jobs.jobs (
      id uuid PRIMARY KEY,
      name text NOT NULL,
      payload jsonb NOT NULL,
      key text,
      state text NOT NULL DEFAULT 'pending'
        CHECK (state IN ('pending', 'running', 'completed', 'failed', 'cancelled')),
      policy jsonb NOT NULL,
      attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
      run_at timestamptz NOT NULL DEFAULT clock_timestamp(),
      created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
      updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
      finished_at timestamptz,
      last_error text,
      claim_token uuid,
      lease_until timestamptz,
      deadline timestamptz,
      CHECK ((state = 'running') = (claim_token IS NOT NULL AND lease_until IS NOT NULL AND deadline IS NOT NULL))
    )`);
    await tx.queryClassic(`CREATE UNIQUE INDEX jobs_outstanding_key
      ON tenon_jobs.jobs (name, key) WHERE key IS NOT NULL AND state IN ('pending', 'running')`);
    await tx.queryClassic(`CREATE INDEX jobs_pending ON tenon_jobs.jobs (name, run_at, id)
      WHERE state = 'pending'`);
    await tx.queryClassic(`CREATE INDEX jobs_expired ON tenon_jobs.jobs (lease_until, id)
      WHERE state = 'running'`);
    await tx.queryClassic('CREATE INDEX jobs_lookup ON tenon_jobs.jobs (name, created_at, id)');
    await tx.queryClassic(`CREATE TABLE tenon_jobs.attempts (
      job_id uuid NOT NULL REFERENCES tenon_jobs.jobs(id) ON DELETE CASCADE,
      number integer NOT NULL,
      token uuid NOT NULL UNIQUE,
      started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
      finished_at timestamptz,
      outcome text CHECK (outcome IN ('completed', 'failed', 'expired')),
      error text,
      PRIMARY KEY (job_id, number)
    )`);
    await tx.queryClassic('INSERT INTO tenon_jobs.migrations (version) VALUES (1)');
  });
}

export async function checkSchema(execute: Execute): Promise<void> {
  const result = await execute<{ version: number }>(
    'SELECT max(version) AS version FROM tenon_jobs.migrations',
  );
  if (result.rows[0]?.version !== 1) throw new Error('Unsupported Tenon Jobs schema version.');
}
