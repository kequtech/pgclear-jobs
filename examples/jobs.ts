import { defineJob } from '@kequtech/pgclear-jobs';

// This module is shared with producers; it imports no handler implementation.
export const greeting = defineJob<{ name: string }>({
  name: 'example.greeting',
  maxAttempts: 3,
  retryDelayMs: 1000,
});
