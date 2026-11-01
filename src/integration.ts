import type { Pool, TransactionOptions } from '@kequtech/pgclear';
import { definitionFactory } from './definition.js';
import type { JobDefinition } from './definition.js';
import { JobRecords } from './records.js';
import { runJobsWithIntegration } from './runner.js';
import type { RunnerOptions } from './runner.js';
import type { Execute } from './storage.js';

export type { Execute } from './storage.js';

/** The caller owns both the root pool and the exact transaction scope passed to a handler. */
export interface JobsIntegration<Database, Tx> {
  pool(database: Database): Pool;
  execute(scope: Database | Tx): Execute;
  transaction<Result>(
    database: Database,
    work: (tx: Tx) => Promise<Result>,
    options: TransactionOptions,
  ): Promise<Result>;
}

export interface JobsApi<Database, Tx> {
  readonly defineJob: ReturnType<typeof definitionFactory<Tx>>;
  jobs(scope: Database | Tx): JobRecords;
  runJobs(
    database: Database,
    definitions: readonly JobDefinition<unknown, Tx>[],
    options?: RunnerOptions,
  ): Promise<void>;
}

/** Bind another pgclear-backed query API without constructing pools or wrapping each handler. */
export function createJobsIntegration<Database, Tx>(
  integration: JobsIntegration<Database, Tx>,
): JobsApi<Database, Tx> {
  return Object.freeze({
    defineJob: definitionFactory<Tx>(),
    jobs: (scope: Database | Tx) => new JobRecords(integration.execute(scope)),
    runJobs: (
      database: Database,
      definitions: readonly JobDefinition<unknown, Tx>[],
      options: RunnerOptions = {},
    ) => runJobsWithIntegration(database, definitions, options, integration),
  });
}
