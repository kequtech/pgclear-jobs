export class JobNotFoundError extends Error {
  constructor(readonly jobId: string) {
    super(`Job ${jobId} does not exist.`);
    this.name = 'JobNotFoundError';
  }
}

export class JobStateError extends Error {
  constructor(readonly jobId: string) {
    super(`Job ${jobId} is no longer eligible for this operation.`);
    this.name = 'JobStateError';
  }
}

export interface JobErrorOptions extends ErrorOptions {
  readonly retry?: boolean;
  /** Do not retry before this instant; ordinary backoff may delay it further. */
  readonly retryAt?: Date;
}

/** Ordinary errors retry automatically; use this only to refine that policy. */
export class JobError extends Error {
  readonly retry: boolean;
  readonly retryAt: string | null;

  constructor(message: string, options: JobErrorOptions = {}) {
    super(message, options);
    this.name = 'JobError';
    if (options.retry !== undefined && typeof options.retry !== 'boolean')
      throw new TypeError('retry must be boolean.');
    if (
      options.retryAt !== undefined &&
      (!(options.retryAt instanceof Date) || !Number.isFinite(options.retryAt.getTime()))
    ) {
      throw new TypeError('retryAt must be a valid Date.');
    }
    if (options.retry === false && options.retryAt !== undefined)
      throw new TypeError('A permanent failure cannot have retryAt.');
    this.retry = options.retry ?? true;
    this.retryAt = options.retryAt?.toISOString() ?? null;
  }
}

export function errorText(error: unknown): string {
  try {
    return (error instanceof Error ? `${error.name}: ${error.message}` : String(error)).slice(
      0,
      4000,
    );
  } catch {
    return 'Handler threw an unprintable error';
  }
}
