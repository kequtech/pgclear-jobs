export interface ExecutionPolicy {
  readonly maxAttempts: number;
  readonly retryDelayMs: number;
  readonly retryBackoff: 'fixed' | 'exponential';
  readonly executionTimeoutMs: number;
}

export interface HandlerContext {
  readonly id: string;
  readonly attempt: number;
  /** Cooperative cancellation on timeout, lost ownership, or shutdown expiry. */
  readonly signal: AbortSignal;
}

export interface JobDefinition<Payload> extends ExecutionPolicy {
  readonly name: string;
  handler(payload: Payload, context: HandlerContext): void | Promise<void>;
}

export interface JobOptions<Payload> extends Partial<ExecutionPolicy> {
  readonly name: string;
  handler(payload: Payload, context: HandlerContext): void | Promise<void>;
}

export function integer(value: number, name: string, min = 1, max = 2_147_483_647): number {
  if (!Number.isSafeInteger(value) || value < min || value > max) {
    throw new TypeError(`${name} must be an integer between ${min} and ${max}.`);
  }
  return value;
}

export function defineJob<Payload>(options: JobOptions<Payload>): JobDefinition<Payload> {
  if (typeof options.name !== 'string' || !options.name.trim() || options.name.length > 200) {
    throw new TypeError('Job name must contain 1–200 characters and cannot be blank.');
  }
  if (typeof options.handler !== 'function') throw new TypeError('A job handler is required.');
  const retryBackoff = options.retryBackoff ?? 'exponential';
  if (retryBackoff !== 'fixed' && retryBackoff !== 'exponential') {
    throw new TypeError('retryBackoff must be fixed or exponential.');
  }
  return Object.freeze({
    name: options.name,
    handler: options.handler,
    maxAttempts: integer(options.maxAttempts ?? 3, 'maxAttempts', 1, 1000),
    retryDelayMs: integer(options.retryDelayMs ?? 1000, 'retryDelayMs', 0, 86_400_000),
    retryBackoff,
    executionTimeoutMs: integer(options.executionTimeoutMs ?? 300_000, 'executionTimeoutMs'),
  });
}

/** Reject lossy JSON conversion, including Dates, sparse arrays and undefined fields. */
export function serializePayload(value: unknown): string {
  const ancestors = new Set<object>();
  function visit(item: unknown): unknown {
    if (item === null || typeof item === 'string' || typeof item === 'boolean') return item;
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
