-- Run as the storage owner after installation. Create jobs_runtime separately.
GRANT USAGE ON SCHEMA pgclear_jobs TO jobs_runtime;
GRANT SELECT ON pgclear_jobs.schema_version TO jobs_runtime;
GRANT SELECT, INSERT, UPDATE, DELETE ON pgclear_jobs.jobs, pgclear_jobs.attempts,
  pgclear_jobs.schedules TO jobs_runtime;
REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA pgclear_jobs FROM PUBLIC;
GRANT EXECUTE ON FUNCTION pgclear_jobs.request_job(uuid, text, jsonb, text,
  timestamptz, double precision, jsonb, boolean) TO jobs_runtime;
