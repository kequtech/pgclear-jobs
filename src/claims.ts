import { randomUUID } from 'node:crypto';
import { errorText, JobError } from './errors.js';
import type { Execute, StoredJob } from './storage.js';

export interface Claim extends StoredJob {
  claim_token: string;
  deadline: string;
}

// Shared by handler failures and abandoned-claim recovery. Older rows retain their defaults.
const remainingAttempts = "j.attempt_count < (j.policy->>'maxAttempts')::integer";
const retryDelay = `LEAST(COALESCE((j.policy->>'retryMaxDelayMs')::double precision, 86400000),
  (j.policy->>'retryDelayMs')::double precision *
  CASE WHEN j.policy->>'retryBackoff' = 'exponential'
    THEN power(2::double precision, LEAST(j.attempt_count - 1, 31)) ELSE 1 END) *
  CASE WHEN COALESCE((j.policy->>'retryJitter')::boolean, false) THEN 0.5 + random() / 2 ELSE 1 END`;
const retryTime = `clock_timestamp() + ${retryDelay} * interval '1 millisecond'`;

function outcomeUpdate(success: string, retry: string, message: string, runAt = retryTime): string {
  return `state = CASE WHEN ${success} THEN 'completed' WHEN ${retry} THEN 'pending' ELSE 'failed' END,
    run_at = CASE WHEN ${success} THEN j.run_at ELSE ${runAt} END,
    finished_at = CASE WHEN ${success} OR NOT (${retry}) THEN clock_timestamp() ELSE NULL END,
    last_error = ${message}, updated_at = clock_timestamp(),
    claim_token = NULL, lease_until = NULL, deadline = NULL`;
}

/** Claim and the next scheduled wakeup share one snapshot, avoiding a due-time race. */
export async function claimJob(
  execute: Execute,
  names: readonly string[],
  leaseMs: number,
): Promise<{ claim: Claim | null; delayMs: number | null }> {
  const { rows } = await execute<{ record: Claim | null; delay_ms: number | null }>(
    `WITH candidate AS (
      SELECT id FROM tenon_jobs.jobs
      WHERE state = 'pending' AND run_at <= statement_timestamp()
        AND name IN (SELECT jsonb_array_elements_text($1::jsonb))
      ORDER BY run_at, id LIMIT 1 FOR UPDATE SKIP LOCKED
    ), claimed AS (
      UPDATE tenon_jobs.jobs j SET state = 'running', claim_token = $2::uuid,
        attempt_count = attempt_count + 1, updated_at = clock_timestamp(),
        deadline = clock_timestamp() + (policy->>'executionTimeoutMs')::double precision * interval '1 millisecond',
        lease_until = clock_timestamp() + LEAST($3::double precision,
          (policy->>'executionTimeoutMs')::double precision) * interval '1 millisecond'
      FROM candidate c WHERE j.id = c.id RETURNING j.*
    ), history AS (
      INSERT INTO tenon_jobs.attempts (job_id, number, token)
      SELECT id, attempt_count, claim_token FROM claimed
    ) SELECT (SELECT to_jsonb(c) FROM claimed c) AS record,
      CASE WHEN EXISTS (SELECT FROM claimed) THEN NULL ELSE (
        SELECT ceil(extract(epoch FROM (min(run_at) - clock_timestamp())) * 1000)::double precision
        FROM tenon_jobs.jobs WHERE state = 'pending' AND run_at > statement_timestamp()
          AND name IN (SELECT jsonb_array_elements_text($1::jsonb))
      ) END AS delay_ms`,
    [JSON.stringify(names), randomUUID(), leaseMs],
  );
  return { claim: rows[0]?.record ?? null, delayMs: rows[0]?.delay_ms ?? null };
}

export async function renewClaim(
  execute: Execute,
  claim: Claim,
  leaseMs: number,
): Promise<boolean> {
  const { rows } = await execute<{ id: string }>(
    `UPDATE tenon_jobs.jobs SET lease_until = LEAST(deadline,
       clock_timestamp() + $3::double precision * interval '1 millisecond')
     WHERE id = $1::uuid AND claim_token = $2::uuid AND state = 'running'
       AND lease_until > clock_timestamp() AND deadline > clock_timestamp() RETURNING id`,
    [claim.id, claim.claim_token, leaseMs],
  );
  return rows.length === 1;
}

/** All outcomes are fenced by the current token and live lease. */
export async function finishClaim(
  execute: Execute,
  claim: Claim,
  error: string | JobError | null,
): Promise<boolean> {
  const { rows } = await execute<{ id: string }>(
    `WITH finished AS (
      UPDATE tenon_jobs.jobs j SET ${outcomeUpdate(
        '$3::text IS NULL',
        `$4 AND ${remainingAttempts}`,
        '$3',
        `GREATEST(${retryTime}, $5::timestamptz)`,
      )}
      WHERE id = $1::uuid AND claim_token = $2::uuid AND state = 'running'
        AND lease_until > clock_timestamp() AND ($3::text IS NOT NULL OR deadline > clock_timestamp())
      RETURNING id
    ), history AS (
      UPDATE tenon_jobs.attempts SET finished_at = clock_timestamp(), error = $3,
        outcome = CASE WHEN $3::text IS NULL THEN 'completed' ELSE 'failed' END
      WHERE token = $2::uuid AND job_id IN (SELECT id FROM finished)
    ) SELECT id FROM finished`,
    [
      claim.id,
      claim.claim_token,
      error instanceof JobError ? errorText(error) : error,
      error instanceof JobError ? error.retry : true,
      error instanceof JobError ? error.retryAt : null,
    ],
  );
  return rows.length === 1;
}

/** Recovery is bounded and serializes with completion and heartbeat updates. */
export async function recoverClaims(execute: Execute, limit = 100): Promise<number> {
  const { rows } = await execute<{ id: string }>(
    `WITH expired AS (
      SELECT id, claim_token FROM tenon_jobs.jobs
      WHERE state = 'running' AND lease_until <= clock_timestamp()
      ORDER BY lease_until, id LIMIT $1 FOR UPDATE SKIP LOCKED
    ), recovered AS (
      UPDATE tenon_jobs.jobs j SET ${outcomeUpdate('false', remainingAttempts, "'Attempt lease expired'")}
      FROM expired e WHERE j.id = e.id RETURNING j.id
    ), history AS (
      UPDATE tenon_jobs.attempts SET outcome = 'expired', error = 'Attempt lease expired',
        finished_at = clock_timestamp()
      WHERE token IN (SELECT claim_token FROM expired) AND job_id IN (SELECT id FROM recovered)
    ) SELECT id FROM recovered`,
    [limit],
  );
  return rows.length;
}
