import { Pool } from 'pg';

import { Job, JobEntry, JobRow, RetriesConfig } from './types';
import { buildClaimQuery, claimJob, completeJob, interruptibleSleep, rowToJob } from './worker';

export class JobIterator<Payload> implements AsyncIterable<JobEntry<Payload>> {
  private readonly claimQuery: string;

  constructor(
    private readonly pool: Pool,
    private readonly jobName: string,
    private readonly signal: AbortSignal,
    private readonly pollIntervalMs: number,
    private readonly ready: Promise<void>,
    retriesConfig?: RetriesConfig,
  ) {
    this.claimQuery = buildClaimQuery(retriesConfig !== undefined);
  }

  [Symbol.asyncIterator](): AsyncIterator<JobEntry<Payload>> {
    return this.iterate();
  }

  private async *iterate(): AsyncGenerator<JobEntry<Payload>> {
    await this.ready;
    while (!this.signal.aborted) {
      const client = await this.pool.connect();
      let row: JobRow | null;
      try {
        row = await claimJob(client, this.jobName, this.claimQuery);
      } catch (err) {
        client.release(true);
        throw err;
      }
      client.release();

      if (!row) {
        await interruptibleSleep(this.pollIntervalMs, this.signal);
        continue;
      }

      const job: Job<Payload> = rowToJob<Payload>(row);
      yield {
        job,
        done: () => completeJob(this.pool, job.id),
      };
    }
  }
}
