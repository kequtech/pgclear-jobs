import { unsafeSql } from '@kequtech/tenon-orm';
import type { QueryClient } from '@kequtech/tenon-orm';
import type { InputValue } from '@kequtech/pgclear';
import { JobRecords } from './records.js';

/** Every statement goes through the supplied Tenon database or transaction. */
export function jobs(scope: QueryClient): JobRecords {
  async function execute<Row extends object>(
    text: string,
    values: readonly InputValue[] = [],
  ): Promise<{ readonly rows: readonly Row[] }> {
    return await scope.query<Row>(unsafeSql<Row>(text, values));
  }
  return new JobRecords(execute);
}
