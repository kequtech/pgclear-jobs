import type { Transaction } from '@kequtech/pgclear';

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

export function integer(value: number, name: string, min = 1, max = 2_147_483_647): number {
  if (!Number.isSafeInteger(value) || value < min || value > max) {
    throw new TypeError(`${name} must be an integer between ${min} and ${max}.`);
  }
  return value;
}

/** PostgreSQL text/JSONB cannot preserve NUL or unpaired UTF-16 surrogates. */
export function databaseText(value: unknown, name: string, max = Infinity): string {
  if (typeof value !== 'string' || value.length > max)
    throw new TypeError(
      `${name} must be a string${max === Infinity ? '' : ` of at most ${max} characters`}.`,
    );
  if (value.includes('\0') || !value.isWellFormed())
    throw new TypeError(`${name} cannot contain NUL or unpaired Unicode surrogates.`);
  return value;
}

/** Reject removed or misspelled options rather than silently changing submission behavior. */
export function validateOptions(options: object, allowed: readonly string[]): void {
  for (const key of Object.keys(options)) {
    if (!allowed.includes(key)) throw new TypeError(`Unknown option: ${key}.`);
  }
}

export function uuid(value: unknown): string {
  if (typeof value !== 'string' || !/^[\da-f]{8}-(?:[\da-f]{4}-){3}[\da-f]{12}$/i.test(value))
    throw new TypeError('Job ID must be a UUID.');
  return value.toLowerCase();
}

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

/** JSON payloads omit absent object fields and reject lossy values such as Dates and sparse arrays. */
export function serializePayload(value: unknown): string {
  const ancestors = new Set<object>();
  function visit(item: unknown): unknown {
    if (typeof item === 'string') return databaseText(item, 'Payload text');
    if (item === null || typeof item === 'boolean') return item;
    if (typeof item === 'number' && Number.isFinite(item)) return item;
    if (typeof item !== 'object' || item === null)
      throw new TypeError('Payload must be JSON data.');
    if (ancestors.has(item)) throw new TypeError('Payload cannot contain cycles.');
    ancestors.add(item);
    let snapshot: unknown;
    if (Array.isArray(item)) {
      if (Reflect.ownKeys(item).length !== item.length + 1) {
        throw new TypeError('Payload arrays must be dense and cannot have extra properties.');
      }
      snapshot = Array.from({ length: item.length }, (_, index) => {
        const descriptor = Object.getOwnPropertyDescriptor(item, String(index));
        if (!descriptor || !('value' in descriptor))
          throw new TypeError('Payload cannot contain accessors.');
        return visit(descriptor.value);
      });
    } else {
      if (
        Object.getPrototypeOf(item) !== Object.prototype &&
        Object.getPrototypeOf(item) !== null
      ) {
        throw new TypeError('Payload objects must be plain JSON objects.');
      }
      if (Reflect.ownKeys(item).length !== Object.keys(item).length) {
        throw new TypeError('Payload cannot contain symbol or non-enumerable properties.');
      }
      const object: Record<string, unknown> = Object.create(null) as Record<string, unknown>;
      for (const key of Object.keys(item)) {
        databaseText(key, 'Payload key');
        const descriptor = Object.getOwnPropertyDescriptor(item, key)!;
        if (!('value' in descriptor)) throw new TypeError('Payload cannot contain accessors.');
        if (descriptor.value !== undefined) object[key] = visit(descriptor.value);
      }
      snapshot = object;
    }
    ancestors.delete(item);
    return snapshot;
  }
  return JSON.stringify(visit(value));
}
