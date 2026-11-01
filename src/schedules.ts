import { randomUUID } from 'node:crypto';
import { CronExpressionParser } from 'cron-parser';
import type { Pool } from '@kequtech/pgclear';
import { databaseText, executionPolicy, integer, serializePayload } from './definition.js';
import type { JobDefinition } from './definition.js';
import type { Execute } from './storage.js';

export interface ScheduleOptions {
  readonly cron: string;
  readonly timeZone?: string;
  readonly key?: string;
  readonly missed?: 'once' | 'skip';
  readonly overlap?: 'skip' | 'allow';
}

export interface Schedule<Payload = unknown> {
  readonly id: string;
  readonly name: string;
  readonly key: string;
  readonly payload: Payload;
  readonly policy: ReturnType<typeof executionPolicy>;
  readonly cron: string;
  readonly timeZone: string;
  readonly missed: 'once' | 'skip';
  readonly overlap: 'skip' | 'allow';
  readonly nextRunAt: Date;
  readonly lastJobId: string | null;
}

interface StoredSchedule {
  id: string;
  name: string;
  key: string;
  payload: unknown;
  cron: string;
  time_zone: string;
  missed: 'once' | 'skip';
  overlap: 'skip' | 'allow';
  next_run_at: string;
  last_job_id: string | null;
  policy: ReturnType<typeof executionPolicy>;
}

function expression(options: Pick<ScheduleOptions, 'cron' | 'timeZone'>, from: Date) {
  if (!(from instanceof Date) || !Number.isFinite(from.getTime()))
    throw new TypeError('from must be a valid Date.');
  if (
    typeof options.cron !== 'string' ||
    options.cron.trim().split(/\s+/).length !== 5 ||
    /\bH/i.test(options.cron)
  ) {
    throw new TypeError('Use a deterministic five-field cron expression.');
  }
  const timeZone = options.timeZone ?? 'UTC';
  if (typeof timeZone !== 'string' || !timeZone.trim())
    throw new TypeError('timeZone must name an IANA time zone.');
  new Intl.DateTimeFormat('en', { timeZone }).format(0);
  return CronExpressionParser.parse(options.cron, { currentDate: from, tz: timeZone });
}

export function previewSchedule(
  options: Pick<ScheduleOptions, 'cron' | 'timeZone'>,
  preview: { readonly from?: Date; readonly count?: number } = {},
): readonly Date[] {
  return expression(options, preview.from ?? new Date())
    .take(integer(preview.count ?? 5, 'count', 1, 100))
    .map((date) => date.toDate());
}

function record<Payload>(row: StoredSchedule): Schedule<Payload> {
  return {
    id: row.id,
    name: row.name,
    key: row.key,
    payload: row.payload as Payload,
    policy: executionPolicy(row.policy),
    cron: row.cron,
    timeZone: row.time_zone,
    missed: row.missed,
    overlap: row.overlap,
    nextRunAt: new Date(row.next_run_at),
    lastJobId: row.last_job_id,
  };
}

export async function saveSchedule<Payload>(
  execute: Execute,
  definition: JobDefinition<Payload>,
  payload: NoInfer<Payload>,
  options: ScheduleOptions,
): Promise<Schedule<Payload>> {
  // Validate before SQL, then calculate against the database clock rather than the producer's clock.
  previewSchedule(options, { count: 1 });
  const key = databaseText(options.key ?? '', 'Schedule key', 500);
  const missed = options.missed ?? 'once',
    overlap = options.overlap ?? 'skip';
  if (!['once', 'skip'].includes(missed) || !['skip', 'allow'].includes(overlap))
    throw new TypeError('Invalid schedule policy.');
  const data = serializePayload(payload);
  const clock = await execute<{ now: string }>('SELECT to_jsonb(clock_timestamp()) AS now');
  const next = previewSchedule(options, { from: new Date(clock.rows[0]!.now), count: 1 })[0]!;
  const { rows } = await execute<{ record: StoredSchedule }>(
    `INSERT INTO tenon_jobs.schedules AS s (id, name, key, payload, policy, cron, time_zone, missed, overlap, next_run_at)
     VALUES ($1, $2, $3, $4::jsonb, $5::jsonb, $6, $7, $8, $9, $10::timestamptz)
     ON CONFLICT (name, key) DO UPDATE SET payload = excluded.payload, policy = excluded.policy,
       next_run_at = CASE WHEN (s.cron, s.time_zone) IS DISTINCT FROM (excluded.cron, excluded.time_zone)
         THEN excluded.next_run_at ELSE s.next_run_at END,
       cron = excluded.cron, time_zone = excluded.time_zone, missed = excluded.missed, overlap = excluded.overlap
     RETURNING to_jsonb(s) AS record`,
    [
      randomUUID(),
      definition.name,
      key,
      data,
      JSON.stringify(executionPolicy(definition)),
      options.cron.trim().replace(/\s+/g, ' '),
      options.timeZone ?? 'UTC',
      missed,
      overlap,
      next.toISOString(),
    ],
  );
  return record<Payload>(rows[0]!.record);
}

export async function listSchedules<Payload>(
  execute: Execute,
  definition: JobDefinition<Payload>,
  limit: number,
): Promise<readonly Schedule<Payload>[]> {
  const { rows } = await execute<{ record: StoredSchedule }>(
    'SELECT to_jsonb(s) AS record FROM tenon_jobs.schedules s WHERE name = $1 ORDER BY key LIMIT $2',
    [definition.name, integer(limit, 'limit', 1, 1000)],
  );
  return rows.map(({ record: row }) => record<Payload>(row));
}

/** Lock schedules, enqueue ordinary jobs and advance cursors in the same bounded transaction. */
export async function advanceSchedules(
  pool: Pick<Pool, 'queryClassic' | 'transaction'>,
  names: readonly string[],
  timeoutMs: number,
): Promise<number> {
  const due = await pool.queryClassic<{ due: boolean }>(
    `SELECT EXISTS (SELECT FROM tenon_jobs.schedules WHERE next_run_at <= statement_timestamp()
      AND name IN (SELECT jsonb_array_elements_text($1::jsonb))) AS due`,
    [JSON.stringify(names)],
    { timeoutMs },
  );
  if (!due.rows[0]?.due) return 0;
  return pool.transaction(
    async (tx) => {
      let created = 0;
      const { rows } = await tx.queryClassic<{ record: StoredSchedule; now: string }>(
        `SELECT to_jsonb(s) AS record, to_jsonb(statement_timestamp()) AS now FROM tenon_jobs.schedules s
       WHERE name IN (SELECT jsonb_array_elements_text($1::jsonb)) AND next_run_at <= statement_timestamp()
       ORDER BY next_run_at, id LIMIT 50 FOR UPDATE SKIP LOCKED`,
        [JSON.stringify(names)],
      );
      for (const { record: schedule, now } of rows) {
        const clock = new Date(now);
        const options = { cron: schedule.cron, timeZone: schedule.time_zone };
        const occurred = new Date(schedule.next_run_at);
        const next = previewSchedule(options, { from: clock, count: 1 })[0]!;
        let jobId: string | null = null;
        if (schedule.missed === 'once' || clock.getTime() - occurred.getTime() < 60_000) {
          const inserted = await tx.queryClassic<{ id: string }>(
            `INSERT INTO tenon_jobs.jobs (id, name, payload, policy, schedule_id, scheduled_for)
           SELECT $1::uuid, $2, $3::jsonb, $4::jsonb, $5::uuid, $6::timestamptz
           WHERE $7::boolean OR NOT EXISTS (SELECT FROM tenon_jobs.jobs
             WHERE schedule_id = $5::uuid AND state IN ('pending', 'running'))
           ON CONFLICT (schedule_id, scheduled_for) DO NOTHING RETURNING id`,
            [
              randomUUID(),
              schedule.name,
              JSON.stringify(schedule.payload),
              JSON.stringify(executionPolicy(schedule.policy)),
              schedule.id,
              occurred.toISOString(),
              schedule.overlap === 'allow',
            ],
          );
          jobId = inserted.rows[0]?.id ?? null;
          if (jobId) created++;
        }
        await tx.queryClassic(
          `UPDATE tenon_jobs.schedules SET next_run_at = $2::timestamptz,
        last_job_id = COALESCE($3::uuid, last_job_id) WHERE id = $1::uuid`,
          [schedule.id, next.toISOString(), jobId],
        );
      }
      return created;
    },
    { timeoutMs },
  );
}
