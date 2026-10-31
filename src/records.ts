import { randomUUID } from 'node:crypto';
import type { Executor, InputValue } from '@kequtech/pgclear';
import { integer, serializePayload } from './definition.js';
import type { JobDefinition } from './definition.js';
import { JobNotFoundError, JobStateError } from './errors.js';
import { jobRecord, states } from './storage.js';
import type { Execute, Job, JobState, StoredJob } from './storage.js';

export interface SendOptions {
  readonly key?: string;
  readonly runAt?: Date;
}

export interface FindOptions {
  readonly key?: string;
  readonly states?: readonly JobState[];
  readonly limit?: number;
}

export interface Attempt {
  readonly number: number;
  readonly startedAt: Date;
  readonly finishedAt: Date | null;
  readonly outcome: 'completed' | 'failed' | 'expired' | null;
  readonly error: string | null;
}

function timestamp(date: Date): string {
  if (!(date instanceof Date) || !Number.isFinite(date.getTime())) {
    throw new TypeError('runAt must be a valid Date.');
  }
  return date.toISOString();
}

export class JobRecords {
  constructor(private readonly execute: Execute) {}

  async send<Payload>(
    definition: JobDefinition<Payload>,
    payload: NoInfer<Payload>,
    options: SendOptions = {},
  ): Promise<Job<Payload>> {
    if (
      options.key !== undefined &&
      (typeof options.key !== 'string' || options.key.length > 500)
    ) {
      throw new TypeError('key must be a string of at most 500 characters.');
    }
    const { maxAttempts, retryDelayMs, retryBackoff, executionTimeoutMs } = definition;
    const result = await this.execute<{ record: StoredJob }>(
      `INSERT INTO tenon_jobs.jobs AS j (id, name, payload, key, run_at, policy)
       VALUES ($1, $2, $3::jsonb, $4, COALESCE($5::timestamptz, clock_timestamp()), $6::jsonb)
       ON CONFLICT (name, key) WHERE key IS NOT NULL AND state IN ('pending', 'running')
       DO UPDATE SET key = j.key
       RETURNING to_jsonb(j) AS record`,
      [
        randomUUID(),
        definition.name,
        serializePayload(payload),
        options.key ?? null,
        options.runAt === undefined ? null : timestamp(options.runAt),
        JSON.stringify({ maxAttempts, retryDelayMs, retryBackoff, executionTimeoutMs }),
      ],
    );
    return jobRecord<Payload>(result.rows[0]!.record);
  }

  async get(id: string): Promise<Job | null> {
    const result = await this.execute<{ record: StoredJob }>(
      'SELECT to_jsonb(j) AS record FROM tenon_jobs.jobs j WHERE id = $1::uuid',
      [id],
    );
    return result.rows[0] ? jobRecord(result.rows[0].record) : null;
  }

  async find<Payload>(
    definition: JobDefinition<Payload>,
    options: FindOptions = {},
  ): Promise<readonly Job<Payload>[]> {
    if (options.states?.some((state) => !states.includes(state)))
      throw new TypeError('Unknown job state.');
    const result = await this.execute<{ record: StoredJob }>(
      `SELECT to_jsonb(j) AS record FROM tenon_jobs.jobs j
       WHERE name = $1 AND ($2::text IS NULL OR key = $2)
       AND ($3::jsonb IS NULL OR state IN (SELECT jsonb_array_elements_text($3)))
       ORDER BY created_at, id LIMIT $4`,
      [
        definition.name,
        options.key ?? null,
        options.states === undefined ? null : JSON.stringify(options.states),
        integer(options.limit ?? 100, 'limit', 1, 1000),
      ],
    );
    return result.rows.map(({ record }) => jobRecord<Payload>(record));
  }

  async reschedule(id: string, runAt: Date): Promise<Job> {
    return this.mutate(id, 'run_at = $2::timestamptz', [timestamp(runAt)]);
  }

  async update<Payload>(
    definition: JobDefinition<Payload>,
    id: string,
    payload: NoInfer<Payload>,
  ): Promise<Job<Payload>> {
    return this.mutate<Payload>(
      id,
      'payload = $2::jsonb',
      [serializePayload(payload)],
      definition.name,
    );
  }

  async cancel(id: string): Promise<Job> {
    return this.mutate(id, "state = 'cancelled', finished_at = clock_timestamp()", []);
  }

  private async mutate<Payload = unknown>(
    id: string,
    assignment: string,
    values: readonly InputValue[],
    name?: string,
  ): Promise<Job<Payload>> {
    const args: InputValue[] = [id, ...values];
    const nameFilter = name === undefined ? '' : ` AND name = $${args.push(name)}`;
    const result = await this.execute<{ record: StoredJob }>(
      `UPDATE tenon_jobs.jobs j SET ${assignment}, updated_at = clock_timestamp()
       WHERE id = $1::uuid AND state = 'pending'${nameFilter} RETURNING to_jsonb(j) AS record`,
      args,
    );
    if (result.rows[0]) return jobRecord<Payload>(result.rows[0].record);
    if (!(await this.get(id))) throw new JobNotFoundError(id);
    throw new JobStateError(id);
  }

  async attempts(
    id: string,
    options: { readonly limit?: number } = {},
  ): Promise<readonly Attempt[]> {
    const { rows } = await this.execute<{
      record: {
        number: number;
        started_at: string;
        finished_at: string | null;
        outcome: Attempt['outcome'];
        error: string | null;
      };
    }>(
      `SELECT to_jsonb(a) AS record FROM tenon_jobs.attempts a
       WHERE job_id = $1::uuid ORDER BY number DESC LIMIT $2`,
      [id, integer(options.limit ?? 100, 'limit', 1, 1000)],
    );
    return rows.map(({ record }) => ({
      number: record.number,
      startedAt: new Date(record.started_at),
      finishedAt: record.finished_at === null ? null : new Date(record.finished_at),
      outcome: record.outcome,
      error: record.error,
    }));
  }
}

export function jobs(scope: Pick<Executor, 'queryClassic'>): JobRecords {
  return new JobRecords((text, values) => scope.queryClassic(text, values));
}
