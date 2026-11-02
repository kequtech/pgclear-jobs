import { randomUUID } from 'node:crypto';
import { errorText, JobError } from './errors.js';
import type { Execute, StoredJob } from './storage.js';

export interface Claim extends StoredJob {
  claim_token: string;
  deadline: string;
}

// Shared by handler failures and abandoned-claim recovery.
const remainingAttempts = "j.attempt_count < (j.policy->>'maxAttempts')::integer";
const retryDelay = `LEAST((j.policy->>'retryMaxDelayMs')::double precision,
  (j.policy->>'retryDelayMs')::double precision *
  CASE WHEN j.policy->>'retryBackoff' = 'exponential'
    THEN power(2::double precision, LEAST(j.attempt_count - 1, 31)) ELSE 1 END) *
  CASE WHEN (j.policy->>'retryJitter')::boolean THEN 0.5 + random() / 2 ELSE 1 END`;
const retryTime = `clock_timestamp() + ${retryDelay} * interval '1 millisecond'`;

function outcomeUpdate(
  success: string,
  retry: string,
  message: string,
  runAt = retryTime,
  cancelled = 'false',
): string {
  return `state = CASE WHEN ${cancelled} THEN 'cancelled' WHEN ${success} THEN 'completed' WHEN ${retry} THEN 'pending' ELSE 'failed' END,
    run_at = CASE WHEN ${cancelled} OR ${success} THEN j.run_at ELSE ${runAt} END,
    finished_at = CASE WHEN ${cancelled} OR ${success} OR NOT (${retry}) THEN clock_timestamp() ELSE NULL END,
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
    `WITH selected AS (
      SELECT name, position FROM jsonb_array_elements_text($1::jsonb) WITH ORDINALITY AS d(name, position)
    ), candidate AS (
      SELECT due.id FROM selected d CROSS JOIN LATERAL (
        SELECT id FROM pgclear_jobs.jobs WHERE name = d.name
          AND state = 'pending' AND blocked_by IS NULL AND run_at <= statement_timestamp()
        ORDER BY run_at, id LIMIT 1 FOR UPDATE SKIP LOCKED
      ) due ORDER BY d.position LIMIT 1
    ), claimed AS (
      UPDATE pgclear_jobs.jobs j SET state = 'running', claim_token = $2::uuid,
        attempt_count = attempt_count + 1, updated_at = clock_timestamp(),
        deadline = clock_timestamp() + (policy->>'executionTimeoutMs')::double precision * interval '1 millisecond',
        lease_until = clock_timestamp() + LEAST($3::double precision,
          (policy->>'executionTimeoutMs')::double precision) * interval '1 millisecond'
      FROM candidate c WHERE j.id = c.id RETURNING j.*
    ), history AS (
      INSERT INTO pgclear_jobs.attempts (job_id, number, token)
      SELECT id, attempt_count, claim_token FROM claimed
    ) SELECT (SELECT to_jsonb(c) FROM claimed c) AS record,
      CASE WHEN EXISTS (SELECT FROM claimed) THEN NULL ELSE (
        SELECT ceil(extract(epoch FROM (min(run_at) - clock_timestamp())) * 1000)::double precision
        FROM selected d CROSS JOIN LATERAL (
          SELECT run_at FROM pgclear_jobs.jobs WHERE name = d.name
            AND state = 'pending' AND blocked_by IS NULL AND run_at > statement_timestamp()
          ORDER BY run_at, id LIMIT 1
        ) future
      ) END AS delay_ms`,
    [JSON.stringify(names), randomUUID(), leaseMs],
  );
  return { claim: rows[0]?.record ?? null, delayMs: rows[0]?.delay_ms ?? null };
}

export async function renewClaim(
  execute: Execute,
  claim: Claim,
  leaseMs: number,
): Promise<boolean | 'cancelled'> {
  // Check time after acquiring the row lock. A lock-only wait does not cause
  // PostgreSQL to re-evaluate predicates already checked on the target row.
  const { rows } = await execute<{ id: string; cancel_requested_at: string | null }>(
    `WITH owned AS MATERIALIZED (
       SELECT id, lease_until, deadline FROM pgclear_jobs.jobs
       WHERE id = $1::uuid AND claim_token = $2::uuid AND state = 'running' FOR UPDATE
     ) UPDATE pgclear_jobs.jobs j SET lease_until = LEAST(owned.deadline,
       clock_timestamp() + $3::double precision * interval '1 millisecond')
     FROM owned WHERE j.id = owned.id
       AND owned.lease_until > clock_timestamp() AND owned.deadline > clock_timestamp() RETURNING j.id, j.cancel_requested_at`,
    [claim.id, claim.claim_token, leaseMs],
  );
  return rows[0]?.cancel_requested_at ? 'cancelled' : rows.length === 1;
}

export interface AttemptOutcome {
  readonly state: 'completed' | 'pending' | 'failed';
  readonly runAt: Date;
}

/** All outcomes are fenced by the current token and live lease. */
export async function finishAttempt(
  execute: Execute,
  claim: Claim,
  error: string | JobError | null,
): Promise<AttemptOutcome | null> {
  const { rows } = await execute<{ state: AttemptOutcome['state']; run_at: string }>(
    `WITH owned AS MATERIALIZED (
      SELECT id, lease_until, deadline, cancel_requested_at FROM pgclear_jobs.jobs
      WHERE id = $1::uuid AND claim_token = $2::uuid AND state = 'running' FOR UPDATE
    ), finished AS (
      UPDATE pgclear_jobs.jobs j SET ${outcomeUpdate(
        '$3::text IS NULL',
        `$4 AND ${remainingAttempts}`,
        '$3',
        `GREATEST(${retryTime}, $5::timestamptz)`,
      )}
      FROM owned WHERE j.id = owned.id
        AND owned.lease_until > clock_timestamp() AND owned.cancel_requested_at IS NULL
        AND ($3::text IS NOT NULL OR owned.deadline > clock_timestamp())
      RETURNING j.id, j.state, to_jsonb(j.run_at) AS run_at
    ), history AS (
      UPDATE pgclear_jobs.attempts SET finished_at = clock_timestamp(), error = $3,
        outcome = CASE WHEN $3::text IS NULL THEN 'completed' ELSE 'failed' END
      WHERE token = $2::uuid AND job_id IN (SELECT id FROM finished)
    ) SELECT state, run_at FROM finished`,
    [
      claim.id,
      claim.claim_token,
      error instanceof JobError ? errorText(error) : error,
      error instanceof JobError ? error.retry : true,
      error instanceof JobError ? error.retryAt : null,
    ],
  );
  return rows[0] ? { state: rows[0].state, runAt: new Date(rows[0].run_at) } : null;
}

export async function finishClaim(
  execute: Execute,
  claim: Claim,
  error: string | JobError | null,
): Promise<boolean> {
  return (await finishAttempt(execute, claim, error)) !== null;
}

/** Recovery is bounded and serializes with completion and heartbeat updates. */
export async function recoverClaims(execute: Execute, limit = 100): Promise<number> {
  const { rows } = await execute<{ id: string }>(
    `WITH expired AS (
      SELECT id, claim_token FROM pgclear_jobs.jobs
      WHERE state = 'running' AND lease_until <= clock_timestamp()
      ORDER BY lease_until, id LIMIT $1 FOR UPDATE SKIP LOCKED
    ), recovered AS (
      UPDATE pgclear_jobs.jobs j SET ${outcomeUpdate(
        'false',
        remainingAttempts,
        "CASE WHEN j.cancel_requested_at IS NOT NULL THEN j.last_error ELSE 'Attempt lease expired' END",
        retryTime,
        'j.cancel_requested_at IS NOT NULL',
      )}
      FROM expired e WHERE j.id = e.id RETURNING j.id, j.cancel_requested_at
    ), history AS (
      UPDATE pgclear_jobs.attempts a SET
        outcome = CASE WHEN r.cancel_requested_at IS NOT NULL THEN 'cancelled' ELSE 'expired' END,
        error = CASE WHEN r.cancel_requested_at IS NOT NULL THEN NULL ELSE 'Attempt lease expired' END,
        finished_at = clock_timestamp()
      FROM recovered r WHERE a.job_id = r.id AND a.token IN (SELECT claim_token FROM expired)
    ) SELECT id FROM recovered`,
    [limit],
  );
  return rows.length;
}

/** Acknowledge cancellation after handler cleanup; recovery handles lost workers. */
export async function finishCancellation(execute: Execute, claim: Claim): Promise<boolean> {
  const { rows } = await execute<{ id: string }>(
    `WITH cancelled AS (
      UPDATE pgclear_jobs.jobs j SET state = 'cancelled', finished_at = clock_timestamp(),
        updated_at = clock_timestamp(), claim_token = NULL, lease_until = NULL, deadline = NULL
      WHERE id = $1::uuid AND claim_token = $2::uuid AND state = 'running'
        AND cancel_requested_at IS NOT NULL RETURNING id
    ), history AS (
      UPDATE pgclear_jobs.attempts SET outcome = 'cancelled', finished_at = clock_timestamp()
      WHERE token = $2::uuid AND job_id IN (SELECT id FROM cancelled)
    ) SELECT id FROM cancelled`,
    [claim.id, claim.claim_token],
  );
  return rows.length === 1;
}
