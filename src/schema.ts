import type { Pool } from '@kequtech/pgclear';
import { downSql, migrationLockSql, schemaVersion, upStatements } from './schema-sql.js';
import type { Execute } from './storage.js';

export { notificationChannel } from './schema-sql.js';

const incompatible =
  'pgclear-jobs schema is incompatible or missing; install current storage or migrate existing storage through your application before starting workers. Never reset existing storage to upgrade.';

/** Explicit installation/removal; neither producers nor workers apply schema changes. */
export async function migrateJobs(
  pool: Pick<Pool, 'transaction'>,
  direction: 'up' | 'down' = 'up',
): Promise<void> {
  if (direction !== 'up' && direction !== 'down')
    throw new TypeError('Migration direction must be up or down.');
  await pool.transaction(async (tx) => {
    await tx.queryClassic(migrationLockSql);
    if (direction === 'down') {
      await tx.queryClassic(downSql);
      return;
    }
    const { rows } = await tx.queryClassic<{ present: boolean }>(
      "SELECT to_regnamespace('pgclear_jobs') IS NOT NULL AS present",
    );
    if (rows[0]!.present) {
      await checkSchema((sql, values) => tx.queryClassic(sql, values));
      return;
    }
    for (const sql of upStatements) await tx.queryClassic(sql);
  });
}

/** Programmatic equivalent of the migrate command. */
export function installJobs(pool: Pick<Pool, 'transaction'>): Promise<void> {
  return migrateJobs(pool);
}

export async function checkSchema(execute: Execute): Promise<void> {
  const { rows } = await execute<{ present: boolean }>(
    "SELECT to_regclass('pgclear_jobs.schema_version') IS NOT NULL AS present",
  );
  if (!rows[0]!.present) throw new Error(incompatible);
  const result = await execute<{ version: number }>(
    'SELECT version FROM pgclear_jobs.schema_version',
  );
  if (result.rows.length !== 1 || result.rows[0]!.version !== schemaVersion)
    throw new Error(incompatible);
}
