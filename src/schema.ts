import type { Pool } from '@kequtech/pgclear';
import type { Execute } from './storage.js';

export const notificationChannel = 'tenon_jobs_ready';
const schemaVersion = 3;

// Unreleased baseline. Disposable older installations must be dropped explicitly, not migrated.
const statements = [
  `CREATE TABLE tenon_jobs.schema_version (version integer PRIMARY KEY)`,
  `INSERT INTO tenon_jobs.schema_version VALUES (${schemaVersion})`,
  `CREATE TABLE tenon_jobs.jobs (
    id uuid PRIMARY KEY,
    name text NOT NULL,
    payload jsonb NOT NULL,
    key text,
    blocked_by uuid,
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
    schedule_id uuid,
    scheduled_for timestamptz,
    retry_of uuid,
    CHECK ((state = 'running') = (claim_token IS NOT NULL AND lease_until IS NOT NULL AND deadline IS NOT NULL)),
    CHECK (blocked_by IS NULL OR (blocked_by <> id AND key IS NOT NULL AND state = 'pending' AND attempt_count = 0))
  )`,
  `CREATE UNIQUE INDEX jobs_outstanding_key ON tenon_jobs.jobs (name, key)
    WHERE key IS NOT NULL AND blocked_by IS NULL AND state IN ('pending', 'running')`,
  `CREATE UNIQUE INDEX jobs_follow_up ON tenon_jobs.jobs (blocked_by)
    WHERE blocked_by IS NOT NULL AND state = 'pending'`,
  `CREATE INDEX jobs_pending ON tenon_jobs.jobs (name, run_at, id)
    WHERE state = 'pending' AND blocked_by IS NULL`,
  `CREATE INDEX jobs_expired ON tenon_jobs.jobs (lease_until, id) WHERE state = 'running'`,
  'CREATE INDEX jobs_lookup ON tenon_jobs.jobs (name, created_at, id)',
  `CREATE UNIQUE INDEX jobs_schedule_occurrence ON tenon_jobs.jobs (schedule_id, scheduled_for)
    WHERE retry_of IS NULL`,
  // Arbitrate both scheduled production and manual retries, including concurrent inserts.
  `CREATE UNIQUE INDEX jobs_schedule_outstanding ON tenon_jobs.jobs (schedule_id)
    WHERE schedule_id IS NOT NULL AND state IN ('pending', 'running')`,
  `CREATE INDEX jobs_terminal ON tenon_jobs.jobs (name, finished_at, id)
    WHERE state IN ('completed', 'failed', 'cancelled')`,
  `CREATE INDEX jobs_retention ON tenon_jobs.jobs (name, finished_at, id)
    WHERE state IN ('completed', 'cancelled')`,
  `CREATE TABLE tenon_jobs.attempts (
    job_id uuid NOT NULL REFERENCES tenon_jobs.jobs(id) ON DELETE CASCADE,
    number integer NOT NULL,
    token uuid NOT NULL UNIQUE,
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    finished_at timestamptz,
    outcome text CHECK (outcome IN ('completed', 'failed', 'expired')),
    error text,
    PRIMARY KEY (job_id, number)
  )`,
  `CREATE TABLE tenon_jobs.schedules (
    id uuid PRIMARY KEY, name text NOT NULL, key text NOT NULL DEFAULT '', payload jsonb NOT NULL,
    policy jsonb NOT NULL, cron text NOT NULL, time_zone text NOT NULL,
    next_run_at timestamptz NOT NULL, last_job_id uuid, UNIQUE (name, key)
  )`,
  'CREATE INDEX schedules_due ON tenon_jobs.schedules (name, next_run_at, id)',
  // Promotion shares the terminal transition's transaction and parent-before-child lock order.
  `CREATE FUNCTION tenon_jobs.release_follow_up() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
      UPDATE tenon_jobs.jobs SET blocked_by = NULL, updated_at = clock_timestamp()
        WHERE blocked_by = NEW.id AND state = 'pending';
      RETURN NULL;
    END;
  $$`,
  `CREATE TRIGGER release_follow_up AFTER UPDATE OF state ON tenon_jobs.jobs
    FOR EACH ROW WHEN (OLD.state IN ('pending', 'running') AND NEW.state IN ('completed', 'failed', 'cancelled'))
    EXECUTE FUNCTION tenon_jobs.release_follow_up()`,
  // Blocked work becomes discoverable when promotion clears blocked_by.
  `CREATE FUNCTION tenon_jobs.notify_pending() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
      IF TG_OP = 'INSERT' OR OLD.state IS DISTINCT FROM NEW.state
        OR OLD.run_at IS DISTINCT FROM NEW.run_at OR OLD.blocked_by IS DISTINCT FROM NEW.blocked_by THEN
        PERFORM pg_notify('${notificationChannel}', NEW.name);
      END IF;
      RETURN NULL;
    END;
  $$`,
  `CREATE TRIGGER notify_pending AFTER INSERT OR UPDATE OF state, run_at, blocked_by
    ON tenon_jobs.jobs FOR EACH ROW WHEN (NEW.state = 'pending' AND NEW.blocked_by IS NULL)
    EXECUTE FUNCTION tenon_jobs.notify_pending()`,
  // Keep row locking and fresh-snapshot retries inside the caller's statement and transaction.
  `CREATE FUNCTION tenon_jobs.request_job(
    p_id uuid, p_name text, p_payload jsonb, p_key text, p_run_at timestamptz,
    p_delay_ms double precision, p_policy jsonb, p_replace_time boolean
  ) RETURNS tenon_jobs.jobs LANGUAGE plpgsql AS $$
    DECLARE
      current_job tenon_jobs.jobs;
      requested_job tenon_jobs.jobs;
    BEGIN
      LOOP
        INSERT INTO tenon_jobs.jobs AS j (id, name, payload, key, run_at, policy)
          VALUES (p_id, p_name, p_payload, p_key,
            COALESCE(p_run_at, clock_timestamp() + p_delay_ms * interval '1 millisecond'), p_policy)
          ON CONFLICT (name, key) WHERE key IS NOT NULL AND blocked_by IS NULL AND state IN ('pending', 'running')
          DO UPDATE SET
            payload = CASE WHEN j.state = 'pending' AND j.attempt_count = 0 THEN excluded.payload ELSE j.payload END,
            run_at = CASE WHEN j.state = 'pending' AND j.attempt_count = 0 AND p_replace_time THEN excluded.run_at ELSE j.run_at END,
            updated_at = CASE WHEN j.state = 'pending' AND j.attempt_count = 0 THEN clock_timestamp() ELSE j.updated_at END
          RETURNING j.* INTO current_job;

        -- Completion during a lock wait may return a terminal head. Its trigger has already
        -- promoted any successor; look up that head with a fresh statement snapshot.
        IF current_job.state NOT IN ('pending', 'running') THEN CONTINUE; END IF;
        IF current_job.attempt_count = 0 THEN RETURN current_job; END IF;

        INSERT INTO tenon_jobs.jobs AS j (id, name, payload, key, blocked_by, run_at, policy)
          VALUES (p_id, p_name, p_payload, p_key, current_job.id,
            COALESCE(p_run_at, clock_timestamp() + p_delay_ms * interval '1 millisecond'), p_policy)
          ON CONFLICT (blocked_by) WHERE blocked_by IS NOT NULL AND state = 'pending'
          DO UPDATE SET payload = excluded.payload,
            run_at = CASE WHEN p_replace_time THEN excluded.run_at ELSE j.run_at END,
            updated_at = clock_timestamp()
          RETURNING j.* INTO requested_job;
        IF requested_job.state <> 'pending' OR requested_job.blocked_by IS NULL THEN CONTINUE; END IF;
        RETURN requested_job;
      END LOOP;
    END;
  $$`,
] as const;

/** Explicit fresh installation. Existing schemas are checked, never upgraded or erased. */
export async function installJobs(pool: Pick<Pool, 'transaction'>): Promise<void> {
  await pool.transaction(async (tx) => {
    await tx.queryClassic('SELECT pg_advisory_xact_lock(187032712, 1)');
    const { rows } = await tx.queryClassic<{ present: boolean }>(
      "SELECT to_regnamespace('tenon_jobs') IS NOT NULL AS present",
    );
    if (rows[0]!.present) {
      await checkSchema((text, values) => tx.queryClassic(text, values));
      return;
    }
    await tx.queryClassic('CREATE SCHEMA tenon_jobs');
    for (const statement of statements) await tx.queryClassic(statement);
  });
}

export async function checkSchema(execute: Execute): Promise<void> {
  const message =
    'Tenon Jobs schema is incompatible; drop the disposable tenon_jobs schema and run installJobs.';
  const table = await execute<{ present: boolean }>(
    "SELECT to_regclass('tenon_jobs.schema_version') IS NOT NULL AS present",
  );
  if (!table.rows[0]?.present) throw new Error(message);
  const result = await execute<{ version: number }>(
    'SELECT version FROM tenon_jobs.schema_version',
  );
  if (result.rows.length !== 1 || result.rows[0]!.version !== schemaVersion)
    throw new Error(message);
}
