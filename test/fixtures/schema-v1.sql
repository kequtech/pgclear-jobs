SELECT pg_advisory_xact_lock(187032712, 1);

CREATE SCHEMA pgclear_jobs;

CREATE TABLE pgclear_jobs.schema_version (version integer PRIMARY KEY);

INSERT INTO pgclear_jobs.schema_version VALUES (1);

CREATE TABLE pgclear_jobs.jobs (
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
  );

CREATE UNIQUE INDEX jobs_outstanding_key ON pgclear_jobs.jobs (name, key)
    WHERE key IS NOT NULL AND blocked_by IS NULL AND state IN ('pending', 'running');

CREATE UNIQUE INDEX jobs_follow_up ON pgclear_jobs.jobs (blocked_by)
    WHERE blocked_by IS NOT NULL AND state = 'pending';

CREATE INDEX jobs_pending ON pgclear_jobs.jobs (name, run_at, id)
    WHERE state = 'pending' AND blocked_by IS NULL;

CREATE INDEX jobs_expired ON pgclear_jobs.jobs (lease_until, id) WHERE state = 'running';

CREATE INDEX jobs_lookup ON pgclear_jobs.jobs (name, created_at, id);

CREATE UNIQUE INDEX jobs_schedule_occurrence ON pgclear_jobs.jobs (schedule_id, scheduled_for)
    WHERE retry_of IS NULL;

CREATE UNIQUE INDEX jobs_schedule_outstanding ON pgclear_jobs.jobs (schedule_id)
    WHERE schedule_id IS NOT NULL AND state IN ('pending', 'running');

CREATE INDEX jobs_terminal ON pgclear_jobs.jobs (name, finished_at, id)
    WHERE state IN ('completed', 'failed', 'cancelled');

CREATE INDEX jobs_retention ON pgclear_jobs.jobs (name, finished_at, id)
    WHERE state IN ('completed', 'cancelled');

CREATE TABLE pgclear_jobs.attempts (
    job_id uuid NOT NULL REFERENCES pgclear_jobs.jobs(id) ON DELETE CASCADE,
    number integer NOT NULL,
    token uuid NOT NULL UNIQUE,
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    finished_at timestamptz,
    outcome text CHECK (outcome IN ('completed', 'failed', 'expired')),
    error text,
    PRIMARY KEY (job_id, number)
  );

CREATE TABLE pgclear_jobs.schedules (
    id uuid PRIMARY KEY, name text NOT NULL, key text NOT NULL DEFAULT '', payload jsonb NOT NULL,
    policy jsonb NOT NULL, cron text NOT NULL, time_zone text NOT NULL,
    next_run_at timestamptz NOT NULL, last_job_id uuid, UNIQUE (name, key)
  );

CREATE INDEX schedules_due ON pgclear_jobs.schedules (name, next_run_at, id);

CREATE FUNCTION pgclear_jobs.release_follow_up() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
      UPDATE pgclear_jobs.jobs SET blocked_by = NULL, updated_at = clock_timestamp()
        WHERE blocked_by = NEW.id AND state = 'pending';
      RETURN NULL;
    END;
  $$;

CREATE TRIGGER release_follow_up AFTER UPDATE OF state ON pgclear_jobs.jobs
    FOR EACH ROW WHEN (OLD.state IN ('pending', 'running') AND NEW.state IN ('completed', 'failed', 'cancelled'))
    EXECUTE FUNCTION pgclear_jobs.release_follow_up();

CREATE FUNCTION pgclear_jobs.notify_pending() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
      IF TG_OP = 'INSERT' OR OLD.state IS DISTINCT FROM NEW.state
        OR OLD.run_at IS DISTINCT FROM NEW.run_at OR OLD.blocked_by IS DISTINCT FROM NEW.blocked_by THEN
        PERFORM pg_notify('pgclear_jobs_ready', NEW.name);
      END IF;
      RETURN NULL;
    END;
  $$;

CREATE TRIGGER notify_pending AFTER INSERT OR UPDATE OF state, run_at, blocked_by
    ON pgclear_jobs.jobs FOR EACH ROW WHEN (NEW.state = 'pending' AND NEW.blocked_by IS NULL)
    EXECUTE FUNCTION pgclear_jobs.notify_pending();

CREATE FUNCTION pgclear_jobs.request_job(
    p_id uuid, p_name text, p_payload jsonb, p_key text, p_run_at timestamptz,
    p_delay_ms double precision, p_policy jsonb, p_replace_time boolean
  ) RETURNS pgclear_jobs.jobs LANGUAGE plpgsql AS $$
    DECLARE
      current_job pgclear_jobs.jobs;
      requested_job pgclear_jobs.jobs;
    BEGIN
      LOOP
        INSERT INTO pgclear_jobs.jobs AS j (id, name, payload, key, run_at, policy)
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

        INSERT INTO pgclear_jobs.jobs AS j (id, name, payload, key, blocked_by, run_at, policy)
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
  $$;
