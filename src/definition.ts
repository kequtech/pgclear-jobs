import type { Transaction } from '@kequtech/pgclear';
import { databaseText, integer, validateOptions } from './validation.js';

export interface ExecutionPolicy {
  readonly maxAttempts: number;
  readonly retryDelayMs: number;
  readonly retryBackoff: 'fixed' | 'exponential';
  readonly retryMaxDelayMs: number;
  readonly retryJitter: boolean;
  readonly executionTimeoutMs: number;
}

export interface HandlerContext {
  readonly id: string;
  readonly attempt: number;
  readonly maxAttempts: number;
  /** Cooperative cancellation on timeout, lost ownership, or shutdown expiry. */
  readonly signal: AbortSignal;
  readonly scheduledFor: Date | null;
}

export interface TransactionalHandlerContext<Tx = Transaction> extends HandlerContext {
  /** Writes through this scope commit atomically with successful job completion. */
  readonly tx: Tx;
}

interface DefinitionOptions {
  readonly name: string;
  /** Per-runner limit for this definition, within the runner's overall limit. */
  readonly concurrency?: number;
}

export type JobOptions<Payload, Tx = Transaction> = DefinitionOptions &
  Partial<ExecutionPolicy> &
  (
    | {
        readonly transactional?: false;
        handler(payload: Payload, context: HandlerContext): void | Promise<void>;
      }
    | {
        readonly transactional: true;
        handler(payload: Payload, context: TransactionalHandlerContext<Tx>): void | Promise<void>;
      }
  );

export type JobDefinition<Payload, Tx = Transaction> = JobOptions<Payload, Tx> & ExecutionPolicy;

/** Bind only the handler transaction type; definitions remain inert. */
export function definitionFactory<Tx>() {
  function defineJob<Payload>(
    options: Extract<JobOptions<Payload, Tx>, { transactional: true }>,
  ): Extract<JobDefinition<Payload, Tx>, { transactional: true }>;
  function defineJob<Payload>(
    options: Extract<JobOptions<Payload, Tx>, { transactional?: false }>,
  ): Extract<JobDefinition<Payload, Tx>, { transactional?: false }>;
  function defineJob<Payload>(options: JobOptions<Payload, Tx>): JobDefinition<Payload, Tx>;
  function defineJob<Payload>(options: JobOptions<Payload, Tx>): JobDefinition<Payload, Tx> {
    validateOptions(options, [
      'name',
      'handler',
      'transactional',
      'concurrency',
      'maxAttempts',
      'retryDelayMs',
      'retryBackoff',
      'retryMaxDelayMs',
      'retryJitter',
      'executionTimeoutMs',
    ]);
    if (typeof options.name !== 'string' || !options.name.trim() || options.name.length > 200) {
      throw new TypeError('Job name must contain 1–200 characters and cannot be blank.');
    }
    if (typeof options.handler !== 'function') throw new TypeError('A job handler is required.');
    if (options.transactional !== undefined && typeof options.transactional !== 'boolean')
      throw new TypeError('transactional must be boolean.');
    return Object.freeze({
      name: databaseText(options.name, 'Job name'),
      ...(options.transactional
        ? { transactional: true as const, handler: options.handler }
        : { handler: options.handler }),
      ...executionPolicy(options),
      ...(options.concurrency === undefined
        ? {}
        : { concurrency: integer(options.concurrency, 'concurrency', 1, 1000) }),
    });
  }
  return defineJob;
}

export const defineJob = definitionFactory<Transaction>();

/** Select only persisted execution policy. */
export function executionPolicy(options: Partial<ExecutionPolicy>): ExecutionPolicy {
  const retryBackoff = options.retryBackoff ?? 'exponential';
  if (retryBackoff !== 'fixed' && retryBackoff !== 'exponential') {
    throw new TypeError('retryBackoff must be fixed or exponential.');
  }
  if (typeof (options.retryJitter ?? true) !== 'boolean')
    throw new TypeError('retryJitter must be boolean.');
  return {
    maxAttempts: integer(options.maxAttempts ?? 3, 'maxAttempts', 1, 1000),
    retryDelayMs: integer(options.retryDelayMs ?? 1000, 'retryDelayMs', 0, 86_400_000),
    retryBackoff,
    retryMaxDelayMs: integer(options.retryMaxDelayMs ?? 86_400_000, 'retryMaxDelayMs', 0),
    retryJitter: options.retryJitter ?? true,
    executionTimeoutMs: integer(options.executionTimeoutMs ?? 300_000, 'executionTimeoutMs'),
  };
}
