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

/** Reject misspelled options before they silently change behavior. */
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
