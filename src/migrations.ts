import { downSql, migrationLockSql, upStatements } from './schema-sql.js';

/** Concrete SQL for an application migration; the caller supplies the transaction. */
export const jobsMigration = Object.freeze({
  up: upStatements.join(';\n\n') + ';\n',
  down: `${migrationLockSql};\n\n${downSql};\n`,
});

/** Apply in a new application-owned transaction after stopping old producers/workers. */
export const jobsUpgrade = Object.freeze({
  from: 1,
  to: 2,
  up: `${migrationLockSql};
DO $$ BEGIN
  IF (SELECT array_agg(version) FROM pgclear_jobs.schema_version) IS DISTINCT FROM ARRAY[1] THEN
    RAISE EXCEPTION 'Expected Jobs storage version 1';
  END IF;
END $$;
ALTER TABLE pgclear_jobs.schedules ADD COLUMN request_key text;
UPDATE pgclear_jobs.schema_version SET version = 2;
`,
  down: `${migrationLockSql};
DO $$ BEGIN
  IF (SELECT array_agg(version) FROM pgclear_jobs.schema_version) IS DISTINCT FROM ARRAY[2] THEN
    RAISE EXCEPTION 'Expected Jobs storage version 2';
  END IF;
  IF EXISTS (SELECT FROM pgclear_jobs.schedules WHERE request_key IS NOT NULL) THEN
    RAISE EXCEPTION 'Remove or convert keyed schedules before reverting Jobs storage';
  END IF;
END $$;
ALTER TABLE pgclear_jobs.schedules DROP COLUMN request_key;
UPDATE pgclear_jobs.schema_version SET version = 1;
`,
});

/** Storage 2 → 3: durable per-job cancellation. Save this SQL in a new application migration. */
export const jobsCancellationUpgrade = Object.freeze({
  from: 2,
  to: 3,
  up: `${migrationLockSql};
DO $$ BEGIN
  IF (SELECT array_agg(version) FROM pgclear_jobs.schema_version) IS DISTINCT FROM ARRAY[2] THEN
    RAISE EXCEPTION 'Expected Jobs storage version 2';
  END IF;
END $$;
ALTER TABLE pgclear_jobs.jobs ADD COLUMN cancel_requested_at timestamptz;
ALTER TABLE pgclear_jobs.attempts DROP CONSTRAINT attempts_outcome_check;
ALTER TABLE pgclear_jobs.attempts ADD CHECK (outcome IN ('completed', 'failed', 'expired', 'cancelled'));
UPDATE pgclear_jobs.schema_version SET version = 3;
`,
  down: `${migrationLockSql};
DO $$ BEGIN
  IF (SELECT array_agg(version) FROM pgclear_jobs.schema_version) IS DISTINCT FROM ARRAY[3] THEN
    RAISE EXCEPTION 'Expected Jobs storage version 3';
  END IF;
  IF EXISTS (SELECT FROM pgclear_jobs.jobs WHERE state = 'running' AND cancel_requested_at IS NOT NULL)
    OR EXISTS (SELECT FROM pgclear_jobs.attempts WHERE outcome = 'cancelled') THEN
    RAISE EXCEPTION 'Finish cancellation and remove cancelled attempt history before reverting Jobs storage';
  END IF;
END $$;
ALTER TABLE pgclear_jobs.attempts DROP CONSTRAINT attempts_outcome_check;
ALTER TABLE pgclear_jobs.attempts ADD CHECK (outcome IN ('completed', 'failed', 'expired'));
ALTER TABLE pgclear_jobs.jobs DROP COLUMN cancel_requested_at;
UPDATE pgclear_jobs.schema_version SET version = 2;
`,
});
