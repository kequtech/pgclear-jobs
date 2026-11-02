import { createPool } from '@kequtech/pgclear';

export function connect() {
  const connection = process.env.DATABASE_URL;
  if (!connection) throw new Error('DATABASE_URL is required.');
  return createPool(connection);
}
