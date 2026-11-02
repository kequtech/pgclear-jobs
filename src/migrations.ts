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
