import { Pool } from 'pg';

import { Job, JobEntry, WorkerContext } from './types';
import {
  buildClaimQuery,
  claimJob,
  completeJob,
  interruptibleSleep,
  rowToJob,
  ClaimResult,
} from './worker';

export class JobIterator<Payload> implements AsyncIterable<JobEntry<Payload>> {
  private readonly claimQuery: string;

  constructor(
    private readonly pool: Pool,
    private readonly jobName: string,
    private readonly signal: AbortSignal,
    private readonly pollIntervalMs: number,
    private readonly ready: Promise<void>,
    private readonly context: WorkerContext,
  ) {
    this.claimQuery = buildClaimQuery(context.retries !== undefined, context.priority);
  }

  [Symbol.asyncIterator](): AsyncIterator<JobEntry<Payload>> {
    return this.iterate();
  }

  private async *iterate(): AsyncGenerator<JobEntry<Payload>> {
    await this.ready;
    while (!this.signal.aborted) {
      const client = await this.pool.connect();
      let claimResult: ClaimResult;
      try {
        const rateLimitConfig = this.context.rateLimits?.get(this.jobName);
        claimResult = await claimJob(client, this.jobName, this.claimQuery, rateLimitConfig);
      } catch (err) {
        client.release(true);
        throw err;
      }
      client.release();

      if (!claimResult.job) {
        await interruptibleSleep(this.pollIntervalMs, this.signal);
        continue;
      }

      const job: Job<Payload> = rowToJob<Payload>(claimResult.job);
      yield {
        job,
        done: () => completeJob(this.pool, job.id),
      };
    }
  }
}
