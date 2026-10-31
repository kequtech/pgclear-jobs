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
