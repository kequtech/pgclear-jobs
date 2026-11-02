import { randomUUID } from 'node:crypto';
import type { Executor, InputValue } from '@kequtech/pgclear';
import { databaseText, integer, serializePayload, uuid, validateOptions } from './validation.js';
import { executionPolicy } from './definition.js';
import type { JobSpec } from './definition.js';
import { JobNotFoundError, JobStateError } from './errors.js';
import { jobRecord, readCursor, states } from './storage.js';
import type { Execute, Job, JobState, StoredJob } from './storage.js';
import { listSchedules, saveSchedule } from './schedules.js';
import type { Schedule, ScheduleOptions } from './schedules.js';

export interface TimingOptions {
  readonly runAt?: Date;
  readonly delayMs?: number;
}

export interface SendOptions extends TimingOptions {
  readonly id?: string;
}
export interface RequestOptions extends TimingOptions {
  readonly key: string;
}
export type Submission<Payload> = SendOptions & { readonly payload: Payload };
export type TerminalState = Extract<JobState, 'completed' | 'failed' | 'cancelled'>;
export interface PruneOptions {
  readonly before: Date;
  readonly states?: readonly TerminalState[];
  readonly limit?: number;
}
export interface JobStats extends Readonly<Record<JobState, number>> {
  readonly total: number;
  readonly ready: number;
  readonly delayed: number;
  readonly blocked: number;
}

export interface FindOptions {
  readonly key?: string;
  readonly states?: readonly JobState[];
  readonly limit?: number;
  readonly after?: string;
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
    throw new TypeError('Expected a valid Date.');
  }
  return date.toISOString();
}

function submission(payload: unknown, options: TimingOptions) {
  if (options.runAt !== undefined && options.delayMs !== undefined)
    throw new TypeError('Use runAt or delayMs, not both.');
  return {
    payload: serializePayload(payload),
    run_at: options.runAt === undefined ? null : timestamp(options.runAt),
    delay_ms: integer(options.delayMs ?? 0, 'delayMs', 0),
  };
}

function sendInput(payload: unknown, options: SendOptions) {
  validateOptions(options, ['id', 'runAt', 'delayMs']);
  return {
    ...submission(payload, options),
    id: options.id === undefined ? randomUUID() : uuid(options.id),
  };
}

export class JobRecords {
  constructor(private readonly execute: Execute) {}

  schedule<Payload>(
    definition: JobSpec<Payload>,
    payload: NoInfer<Payload>,
    options: ScheduleOptions,
  ): Promise<Schedule<Payload>> {
    return saveSchedule(this.execute, definition, payload, options);
  }

  schedules<Payload>(
    definition: JobSpec<Payload>,
    options: { readonly limit?: number } = {},
  ): Promise<readonly Schedule<Payload>[]> {
    return listSchedules(this.execute, definition, options.limit ?? 100);
  }

  async unschedule(definition: JobSpec<unknown>, key = ''): Promise<boolean> {
    return (
      (
        await this.execute<{ id: string }>(
          'DELETE FROM pgclear_jobs.schedules WHERE name = $1 AND key = $2 RETURNING id',
          [definition.name, databaseText(key, 'Schedule key', 500)],
        )
      ).rows.length === 1
    );
  }

  async send<Payload>(
    definition: JobSpec<Payload>,
    payload: NoInfer<Payload>,
    options: SendOptions = {},
  ): Promise<Job<Payload>> {
    const input = sendInput(payload, options);
    const { rows } = await this.execute<{ record: StoredJob }>(
      `INSERT INTO pgclear_jobs.jobs AS j (id, name, payload, run_at, policy)
       VALUES ($1, $2, $3::jsonb, COALESCE($4::timestamptz,
         clock_timestamp() + $5::double precision * interval '1 millisecond'), $6::jsonb)
       ON CONFLICT (id) DO UPDATE SET id = j.id RETURNING to_jsonb(j) AS record`,
      [
        input.id,
        definition.name,
        input.payload,
        input.run_at,
        input.delay_ms,
        JSON.stringify(executionPolicy(definition)),
      ],
    );
    const job = jobRecord<Payload>(rows[0]!.record);
    if (job.name !== definition.name) throw new JobStateError(job.id);
    return job;
  }

  /** Coalesce unstarted work, or retain one follow-up behind a job and all its retries. */
  async request<Payload>(
    definition: JobSpec<Payload>,
    payload: NoInfer<Payload>,
    options: RequestOptions,
  ): Promise<Job<Payload>> {
    validateOptions(options, ['key', 'runAt', 'delayMs']);
    const key = databaseText(options.key, 'key', 500);
    const input = submission(payload, options);
    const { rows } = await this.execute<{ record: StoredJob }>(
      `SELECT to_jsonb(pgclear_jobs.request_job($1::uuid, $2::text, $3::jsonb, $4::text,
        $5::timestamptz, $6::double precision, $7::jsonb, $8::boolean)) AS record`,
      [
        randomUUID(),
        definition.name,
        input.payload,
        key,
        input.run_at,
        input.delay_ms,
        JSON.stringify(executionPolicy(definition)),
        options.runAt !== undefined || options.delayMs !== undefined,
      ],
    );
    return jobRecord<Payload>(rows[0]!.record);
  }

  async sendMany<Payload>(
    definition: JobSpec<Payload>,
    inputs: readonly Submission<NoInfer<Payload>>[],
  ): Promise<readonly Job<Payload>[]> {
    integer(inputs.length, 'batch size', 0, 1000);
    if (!inputs.length) return [];
    const prepared = Array.from(inputs, ({ payload, ...options }) => {
      const input = sendInput(payload, options);
      return { ...input, payload: JSON.parse(input.payload) as unknown };
    });
    const { rows } = await this.execute<{ record: StoredJob }>(
      `INSERT INTO pgclear_jobs.jobs AS j (id, name, payload, run_at, policy)
       SELECT x.id, $1, COALESCE(x.payload, 'null'::jsonb),
         COALESCE(x.run_at, clock_timestamp() + x.delay_ms * interval '1 millisecond'), $3::jsonb
       FROM jsonb_to_recordset($2::jsonb) AS x(id uuid, payload jsonb, run_at timestamptz, delay_ms double precision)
       ON CONFLICT DO NOTHING RETURNING to_jsonb(j) AS record`,
      [definition.name, JSON.stringify(prepared), JSON.stringify(executionPolicy(definition))],
    );
    return rows.map(({ record }) => jobRecord<Payload>(record));
  }

  async get(id: string): Promise<Job | null> {
    const result = await this.execute<{ record: StoredJob }>(
      'SELECT to_jsonb(j) AS record FROM pgclear_jobs.jobs j WHERE id = $1::uuid',
      [uuid(id)],
    );
    return result.rows[0] ? jobRecord(result.rows[0].record) : null;
  }

  /** Found records once each in first-requested order; missing IDs are omitted. */
  async getMany(ids: readonly string[]): Promise<readonly Job[]> {
    integer(ids.length, 'batch size', 0, 1000);
    const selected = [...new Set(Array.from(ids, uuid))];
    if (!selected.length) return [];
    const { rows } = await this.execute<{ record: StoredJob }>(
      `SELECT to_jsonb(j) AS record FROM jsonb_array_elements_text($1::jsonb)
       WITH ORDINALITY AS requested(id, position)
       JOIN pgclear_jobs.jobs j ON j.id = requested.id::uuid ORDER BY requested.position`,
      [JSON.stringify(selected)],
    );
    return rows.map(({ record }) => jobRecord(record));
  }

  async find<Payload>(
    definition: JobSpec<Payload>,
    options: FindOptions = {},
  ): Promise<readonly Job<Payload>[]> {
    if (options.states?.some((state) => !states.includes(state)))
      throw new TypeError('Unknown job state.');
    const after = options.after === undefined ? null : readCursor(options.after);
    const result = await this.execute<{ record: StoredJob }>(
      `SELECT to_jsonb(j) AS record FROM pgclear_jobs.jobs j
       WHERE name = $1 AND ($2::text IS NULL OR key = $2)
       AND ($3::jsonb IS NULL OR state IN (SELECT jsonb_array_elements_text($3)))
       AND ($5::timestamptz IS NULL OR (created_at, id) > ($5::timestamptz, $6::uuid))
       ORDER BY created_at, id LIMIT $4`,
      [
        definition.name,
        options.key === undefined ? null : databaseText(options.key, 'key', 500),
        options.states === undefined ? null : JSON.stringify(options.states),
        integer(options.limit ?? 100, 'limit', 1, 1000),
        after?.[0] ?? null,
        after?.[1] ?? null,
      ],
    );
    return result.rows.map(({ record }) => jobRecord<Payload>(record));
  }

  async reschedule(id: string, runAt: Date): Promise<Job> {
    return this.mutate(id, 'run_at = $2::timestamptz', [timestamp(runAt)]);
  }

  async update<Payload>(
    definition: JobSpec<Payload>,
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
    return this.mutate(
      id,
      "state = 'cancelled', blocked_by = NULL, finished_at = clock_timestamp()",
      [],
    );
  }

  /** A fresh budget and linked job; an occupied key or schedule rejects the retry. */
  async retry(id: string): Promise<Job> {
    const { rows } = await this.execute<{ record: StoredJob }>(
      `WITH source AS MATERIALIZED (
         SELECT * FROM pgclear_jobs.jobs WHERE id = $1::uuid AND state = 'failed' FOR SHARE
       ) INSERT INTO pgclear_jobs.jobs AS j (id, name, payload, key, policy, retry_of, schedule_id, scheduled_for)
       SELECT $2::uuid, name, payload, key, policy, id, schedule_id, scheduled_for FROM source
       ON CONFLICT DO NOTHING RETURNING to_jsonb(j) AS record`,
      [uuid(id), randomUUID()],
    );
    if (rows[0]) return jobRecord(rows[0].record);
    if (!(await this.get(id))) throw new JobNotFoundError(id);
    throw new JobStateError(id);
  }

  async remove(id: string): Promise<boolean> {
    const { rows } = await this.execute<{ id: string }>(
      `DELETE FROM pgclear_jobs.jobs WHERE id = $1::uuid
       AND state IN ('completed', 'failed', 'cancelled') RETURNING id`,
      [uuid(id)],
    );
    if (rows.length) return true;
    if (await this.get(id)) throw new JobStateError(id);
    return false;
  }

  async prune(definition: JobSpec<unknown>, options: PruneOptions): Promise<number> {
    const selected = options.states ?? ['completed', 'cancelled'];
    if (selected.some((state) => !['completed', 'failed', 'cancelled'].includes(state)))
      throw new TypeError('Only terminal job states can be pruned.');
    const { rows } = await this.execute<{ id: string }>(
      `WITH candidates AS (
         SELECT id FROM pgclear_jobs.jobs WHERE name = $1 AND finished_at < $2::timestamptz
           AND state IN ('completed', 'failed', 'cancelled')
           AND state IN (SELECT jsonb_array_elements_text($3::jsonb))
         ORDER BY finished_at, id LIMIT $4 FOR UPDATE SKIP LOCKED
       ) DELETE FROM pgclear_jobs.jobs j USING candidates c WHERE j.id = c.id RETURNING j.id`,
      [
        definition.name,
        timestamp(options.before),
        JSON.stringify(selected),
        integer(options.limit ?? 100, 'limit', 1, 1000),
      ],
    );
    return rows.length;
  }

  async stats(definition: JobSpec<unknown>): Promise<JobStats> {
    const { rows } = await this.execute<{
      state: JobState;
      count: number;
      ready: number;
      blocked: number;
    }>(
      `SELECT state, count(*)::double precision AS count,
         count(*) FILTER (WHERE state = 'pending' AND blocked_by IS NULL AND run_at <= statement_timestamp())::double precision AS ready,
         count(*) FILTER (WHERE state = 'pending' AND blocked_by IS NOT NULL)::double precision AS blocked
       FROM pgclear_jobs.jobs WHERE name = $1 GROUP BY state`,
      [definition.name],
    );
    const counts: Record<JobState, number> = {
      pending: 0,
      running: 0,
      completed: 0,
      failed: 0,
      cancelled: 0,
    };
    let ready = 0;
    let blocked = 0;
    for (const row of rows) {
      counts[row.state] = row.count;
      ready += row.ready;
      blocked += row.blocked;
    }
    return {
      ...counts,
      ready,
      delayed: counts.pending - ready - blocked,
      blocked,
      total: rows.reduce((sum, row) => sum + row.count, 0),
    };
  }

  private async mutate<Payload = unknown>(
    id: string,
    assignment: string,
    values: readonly InputValue[],
    name?: string,
  ): Promise<Job<Payload>> {
    const args: InputValue[] = [uuid(id), ...values];
    const nameFilter = name === undefined ? '' : ` AND name = $${args.push(name)}`;
    const result = await this.execute<{ record: StoredJob }>(
      `UPDATE pgclear_jobs.jobs j SET ${assignment}, updated_at = clock_timestamp()
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
      `SELECT to_jsonb(a) AS record FROM pgclear_jobs.attempts a
       WHERE job_id = $1::uuid ORDER BY number DESC LIMIT $2`,
      [uuid(id), integer(options.limit ?? 100, 'limit', 1, 1000)],
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
