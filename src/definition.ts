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

export interface JobDefinition<Payload> extends ExecutionPolicy {
  readonly name: string;
  /** Per-runner limit for this definition, within the runner's overall limit. */
  readonly concurrency?: number;
  handler(payload: Payload, context: HandlerContext): void | Promise<void>;
}

export interface JobOptions<Payload> extends Partial<ExecutionPolicy> {
  readonly name: string;
  readonly concurrency?: number;
  handler(payload: Payload, context: HandlerContext): void | Promise<void>;
}

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

export function defineJob<Payload>(options: JobOptions<Payload>): JobDefinition<Payload> {
  if (typeof options.name !== 'string' || !options.name.trim() || options.name.length > 200) {
    throw new TypeError('Job name must contain 1–200 characters and cannot be blank.');
  }
  if (typeof options.handler !== 'function') throw new TypeError('A job handler is required.');
  return Object.freeze({
    name: databaseText(options.name, 'Job name'),
    handler: options.handler,
    ...executionPolicy(options),
    ...(options.concurrency === undefined
      ? {}
      : { concurrency: integer(options.concurrency, 'concurrency', 1, 1000) }),
  });
}

/** Select only persisted execution policy; also supplies defaults for older records. */
export function executionPolicy(options: Partial<ExecutionPolicy>): ExecutionPolicy {
  const retryBackoff = options.retryBackoff ?? 'exponential';
  if (retryBackoff !== 'fixed' && retryBackoff !== 'exponential') {
    throw new TypeError('retryBackoff must be fixed or exponential.');
  }
  if (typeof (options.retryJitter ?? false) !== 'boolean')
    throw new TypeError('retryJitter must be boolean.');
  return {
    maxAttempts: integer(options.maxAttempts ?? 3, 'maxAttempts', 1, 1000),
    retryDelayMs: integer(options.retryDelayMs ?? 1000, 'retryDelayMs', 0, 86_400_000),
    retryBackoff,
    retryMaxDelayMs: integer(options.retryMaxDelayMs ?? 86_400_000, 'retryMaxDelayMs', 0),
    retryJitter: options.retryJitter ?? false,
    executionTimeoutMs: integer(options.executionTimeoutMs ?? 300_000, 'executionTimeoutMs'),
  };
}

/** Reject lossy JSON conversion, including Dates, sparse arrays and undefined fields. */
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
        object[key] = visit(descriptor.value);
      }
      snapshot = object;
    }
    ancestors.delete(item);
    return snapshot;
  }
  return JSON.stringify(visit(value));
}
