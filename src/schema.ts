import type { Pool } from '@kequtech/pgclear';
import type { Execute } from './storage.js';

export const notificationChannel = 'tenon_jobs_ready';

const migrations = [
  [
    `CREATE TABLE tenon_jobs.jobs (
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
    )`,
    `CREATE UNIQUE INDEX jobs_outstanding_key
      ON tenon_jobs.jobs (name, key) WHERE key IS NOT NULL AND state IN ('pending', 'running')`,
    `CREATE INDEX jobs_pending ON tenon_jobs.jobs (name, run_at, id) WHERE state = 'pending'`,
    `CREATE INDEX jobs_expired ON tenon_jobs.jobs (lease_until, id) WHERE state = 'running'`,
    'CREATE INDEX jobs_lookup ON tenon_jobs.jobs (name, created_at, id)',
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
  ],
  [
    // The trigger keeps inserts, retries, recovery and reschedules transactional,
    // including statements issued through a caller's Tenon transaction.
    `CREATE FUNCTION tenon_jobs.notify_pending() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        IF TG_OP = 'INSERT' OR OLD.state IS DISTINCT FROM NEW.state
          OR OLD.run_at IS DISTINCT FROM NEW.run_at THEN
          PERFORM pg_notify('${notificationChannel}', NEW.name);
        END IF;
        RETURN NULL;
      END;
    $$`,
    `CREATE TRIGGER notify_pending AFTER INSERT OR UPDATE OF state, run_at
      ON tenon_jobs.jobs FOR EACH ROW WHEN (NEW.state = 'pending')
      EXECUTE FUNCTION tenon_jobs.notify_pending()`,
  ],
] as const;

/** Explicit, transactional installation and upgrades. Never called by record operations. */
export async function installJobs(pool: Pick<Pool, 'transaction'>): Promise<void> {
  await pool.transaction(async (tx) => {
    await tx.queryClassic('SELECT pg_advisory_xact_lock(187032712, 1)');
    await tx.queryClassic('CREATE SCHEMA IF NOT EXISTS tenon_jobs');
    await tx.queryClassic(`CREATE TABLE IF NOT EXISTS tenon_jobs.migrations (
      version integer PRIMARY KEY, installed_at timestamptz NOT NULL DEFAULT clock_timestamp()
    )`);
    const { rows } = await tx.queryClassic<{ version: number | null }>(
      'SELECT max(version) AS version FROM tenon_jobs.migrations',
    );
    const version = rows[0]?.version ?? 0;
    if (version < 0 || version > migrations.length) {
      throw new Error('Unsupported Tenon Jobs schema version.');
    }
    for (let index = version; index < migrations.length; index++) {
      for (const statement of migrations[index]!) await tx.queryClassic(statement);
      await tx.queryClassic('INSERT INTO tenon_jobs.migrations (version) VALUES ($1)', [index + 1]);
    }
  });
}

export async function checkSchema(execute: Execute): Promise<void> {
  const result = await execute<{ version: number }>(
    'SELECT max(version) AS version FROM tenon_jobs.migrations',
  );
  if (result.rows[0]?.version !== migrations.length) {
    throw new Error('Tenon Jobs schema is incompatible; run installJobs with the current library.');
  }
}
