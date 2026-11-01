import type { Pool, Transaction } from '@kequtech/pgclear';
import type { JobsIntegration } from './integration.js';

export const pgclearIntegration: JobsIntegration<Pool, Transaction> = {
  pool: (pool) => pool,
  execute: (scope) => (text, values) => scope.queryClassic(text, values),
  transaction: (pool, work, options) => pool.transaction(work, options),
};
