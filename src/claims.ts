import { randomUUID } from 'node:crypto';
import type { Execute, StoredJob } from './storage.js';

export interface Claim extends StoredJob {
  claim_token: string;
  deadline: string;
}

// Retry delay is capped at one day; attempt 1 uses the base delay.
const retryDelay = `LEAST(86400000::double precision,
  (j.policy->>'retryDelayMs')::double precision *
  CASE WHEN j.policy->>'retryBackoff' = 'exponential'
    THEN power(2::double precision, LEAST(j.attempt_count - 1, 30)) ELSE 1 END)`;

export async function claimJob(
  execute: Execute,
  names: readonly string[],
  leaseMs: number,
): Promise<Claim | null> {
  const { rows } = await execute<{ record: Claim }>(
    `WITH candidate AS (
      SELECT id FROM tenon_jobs.jobs
      WHERE state = 'pending' AND run_at <= clock_timestamp()
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
    ) SELECT to_jsonb(claimed) AS record FROM claimed`,
    [JSON.stringify(names), randomUUID(), leaseMs],
  );
  return rows[0]?.record ?? null;
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
  error: string | null,
): Promise<boolean> {
  const { rows } = await execute<{ id: string }>(
    `WITH finished AS (
      UPDATE tenon_jobs.jobs j SET
        state = CASE WHEN $3::text IS NULL THEN 'completed'
          WHEN attempt_count < (policy->>'maxAttempts')::integer THEN 'pending' ELSE 'failed' END,
        run_at = CASE WHEN $3::text IS NULL THEN run_at
          ELSE clock_timestamp() + ${retryDelay} * interval '1 millisecond' END,
        finished_at = CASE WHEN $3::text IS NULL OR attempt_count >= (policy->>'maxAttempts')::integer
          THEN clock_timestamp() ELSE NULL END,
        last_error = $3, updated_at = clock_timestamp(),
        claim_token = NULL, lease_until = NULL, deadline = NULL
      WHERE id = $1::uuid AND claim_token = $2::uuid AND state = 'running'
        AND lease_until > clock_timestamp() AND ($3::text IS NOT NULL OR deadline > clock_timestamp())
      RETURNING id
    ), history AS (
      UPDATE tenon_jobs.attempts SET finished_at = clock_timestamp(), error = $3,
        outcome = CASE WHEN $3::text IS NULL THEN 'completed' ELSE 'failed' END
      WHERE token = $2::uuid AND job_id IN (SELECT id FROM finished)
    ) SELECT id FROM finished`,
    [claim.id, claim.claim_token, error],
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
      UPDATE tenon_jobs.jobs j SET
        state = CASE WHEN attempt_count < (policy->>'maxAttempts')::integer THEN 'pending' ELSE 'failed' END,
        run_at = clock_timestamp() + ${retryDelay} * interval '1 millisecond',
        finished_at = CASE WHEN attempt_count >= (policy->>'maxAttempts')::integer THEN clock_timestamp() ELSE NULL END,
        updated_at = clock_timestamp(), last_error = 'Attempt lease expired',
        claim_token = NULL, lease_until = NULL, deadline = NULL
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
