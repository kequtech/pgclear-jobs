import type { Execute } from './storage.js';

/** One bounded batch using the DB clock. Outstanding work and failures are never expired. */
export async function pruneCompleted(
  execute: Execute,
  names: readonly string[],
  retentionMs: number,
): Promise<number> {
  const { rows } = await execute<{ id: string }>(
    `WITH candidates AS (
      SELECT old.id FROM jsonb_array_elements_text($1::jsonb) AS d(name)
      CROSS JOIN LATERAL (
        SELECT id, finished_at FROM pgclear_jobs.jobs WHERE name = d.name
          AND state IN ('completed', 'cancelled')
          AND finished_at < statement_timestamp() - $2::double precision * interval '1 millisecond'
        ORDER BY finished_at, id LIMIT 100 FOR UPDATE SKIP LOCKED
      ) old ORDER BY old.finished_at, old.id LIMIT 100
    ) DELETE FROM pgclear_jobs.jobs j USING candidates c WHERE j.id = c.id RETURNING j.id`,
    [JSON.stringify(names), retentionMs],
  );
  return rows.length;
}
