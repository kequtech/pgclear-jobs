import { createTenon, unsafeSql } from '@kequtech/tenon-orm';
import type { QueryClient } from '@kequtech/tenon-orm';
import type { InputValue, Pool, Transaction } from '@kequtech/pgclear';
import { JobRecords } from './records.js';
import { defineJob as defineCoreJob } from './definition.js';
import type {
  JobDefinition,
  JobOptions as CoreJobOptions,
  TransactionalHandlerContext as CoreTransactionalHandlerContext,
} from './definition.js';

// One entry point for Tenon consumers; the local jobs export supplies the adapted scope.
export * from './index.js';

export type JobOptions<Payload> = CoreJobOptions<Payload, QueryClient>;
export type TransactionalHandlerContext = CoreTransactionalHandlerContext<QueryClient>;

/** Use Tenon's public query policies over the runner's existing pgclear transaction. */
async function withTenonTransaction(
  tx: Transaction,
  work: (tx: QueryClient) => Promise<void> | void,
): Promise<void> {
  const unavailable = async (): Promise<never> => {
    throw new Error('A handler transaction cannot acquire another session.');
  };
  // This borrowed pool capability cannot open or close connections. Tenon's transaction
  // callback supplies its normal query client while the runner retains commit ownership.
  const scoped: Pool = {
    codecs: tx.codecs,
    query: tx.query.bind(tx),
    queryClassic: tx.queryClassic.bind(tx),
    describe: tx.describe.bind(tx),
    transaction: (callback) => callback(tx),
    acquire: unavailable,
    withSession: unavailable,
    close: async () => {},
    stats: { total: 1, leased: 1, idle: 0, waiting: 0 },
  };
  const db = createTenon({ connection: scoped });
  await db.transaction(async (scope) => work(scope));
}

/** Transactional handlers receive a Tenon scope; the core runner still owns its transaction. */
export function defineJob<Payload>(
  options: Extract<JobOptions<Payload>, { transactional: true }>,
): Extract<JobDefinition<Payload>, { transactional: true }>;
export function defineJob<Payload>(
  options: Extract<JobOptions<Payload>, { transactional?: false }>,
): Extract<JobDefinition<Payload>, { transactional?: false }>;
export function defineJob<Payload>(options: JobOptions<Payload>): JobDefinition<Payload>;
export function defineJob<Payload>(options: JobOptions<Payload>): JobDefinition<Payload> {
  if (typeof options.handler !== 'function') throw new TypeError('A job handler is required.');
  if (options.transactional !== undefined && typeof options.transactional !== 'boolean')
    throw new TypeError('transactional must be boolean.');
  if (!options.transactional) return defineCoreJob(options);
  const handler = options.handler;
  return defineCoreJob({
    ...options,
    transactional: true,
    handler: (payload: Payload, context: CoreTransactionalHandlerContext) =>
      withTenonTransaction(context.tx, (tx) => handler(payload, { ...context, tx })),
  });
}

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
