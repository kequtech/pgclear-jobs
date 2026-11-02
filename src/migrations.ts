import { downSql, migrationLockSql, upStatements } from './schema-sql.js';

/** Concrete SQL for an application migration; the caller supplies the transaction. */
export const jobsMigration = Object.freeze({
  up: upStatements.join(';\n\n') + ';\n',
  down: `${migrationLockSql};\n\n${downSql};\n`,
});
