/** A generation counter closes the race between checking storage and going idle. */
export class Wakeup {
  version = 0;
  private readonly waiters = new Set<() => void>();

  notify(): void {
    this.version++;
    for (const finish of this.waiters) finish();
  }

  async wait(version: number, ms: number, signal: AbortSignal): Promise<void> {
    if (signal.aborted || version !== this.version) return;
    await new Promise<void>((resolve) => {
      const finish = () => {
        clearTimeout(timer);
        this.waiters.delete(finish);
        signal.removeEventListener('abort', finish);
        resolve();
      };
      const timer = setTimeout(finish, ms);
      this.waiters.add(finish);
      signal.addEventListener('abort', finish, { once: true });
    });
  }
}

/** Keep a handler's slot until it settles, or shutdown deliberately abandons it. */
export async function settleOrAbort<T>(
  work: Promise<T>,
  signal: AbortSignal,
): Promise<T | undefined> {
  if (signal.aborted) return;
  let onAbort: () => void = () => {};
  const aborted = new Promise<undefined>((resolve) => {
    onAbort = () => resolve(undefined);
    signal.addEventListener('abort', onAbort, { once: true });
  });
  try {
    return await Promise.race([work, aborted]);
  } finally {
    signal.removeEventListener('abort', onAbort);
  }
}
