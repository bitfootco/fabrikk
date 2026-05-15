import { Pool, PoolClient } from 'pg';

import { Job, JobRow, WorkerHandler } from './types';

const CLAIM_QUERY = `
  SELECT id, name, payload, status, attempts, max_attempts, error,
         created_at, started_at, completed_at, failed_at
  FROM fabrikk_jobs
  WHERE name = $1 AND status = 'pending'
  ORDER BY created_at ASC
  LIMIT 1
  FOR UPDATE SKIP LOCKED
`;

export function rowToJob<Payload>(row: JobRow): Job<Payload> {
  return {
    id: row.id,
    name: row.name,
    payload: row.payload as Payload,
    status: row.status,
    attempts: row.attempts,
    max_attempts: row.max_attempts,
    error: row.error,
    created_at: row.created_at,
    started_at: row.started_at,
    completed_at: row.completed_at,
    failed_at: row.failed_at,
  };
}

// Resolves when signal fires or ms elapses, whichever comes first
export function interruptibleSleep(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal.aborted) {
      resolve();
      return;
    }
    const timer = setTimeout(resolve, ms);
    signal.addEventListener(
      'abort',
      () => {
        clearTimeout(timer);
        resolve();
      },
      { once: true },
    );
  });
}

// Claims the next pending job for `name` inside a transaction.
// Returns null (and rolls back) if none available.
// Caller is responsible for releasing the client after this returns.
export async function claimJob(client: PoolClient, name: string): Promise<JobRow | null> {
  await client.query('BEGIN');
  const result = await client.query<JobRow>(CLAIM_QUERY, [name]);
  if (result.rows.length === 0) {
    await client.query('ROLLBACK');
    return null;
  }
  const row = result.rows[0];
  await client.query(
    `UPDATE fabrikk_jobs SET status = 'running', started_at = NOW(), attempts = attempts + 1 WHERE id = $1`,
    [row.id],
  );
  await client.query('COMMIT');
  return row;
}

export async function completeJob(pool: Pool, id: string): Promise<void> {
  await pool.query(
    `UPDATE fabrikk_jobs SET status = 'completed', completed_at = NOW() WHERE id = $1`,
    [id],
  );
}

export async function failJob(pool: Pool, id: string, error: unknown): Promise<void> {
  const message = error instanceof Error ? error.message : String(error);
  await pool.query(
    `UPDATE fabrikk_jobs SET status = 'failed', failed_at = NOW(), error = $2 WHERE id = $1`,
    [id, message],
  );
}

// Minimal interface used by Queue to track workers without binding to the generic
interface IWorker {
  wait(): Promise<void>;
}

export class Worker<Payload> implements IWorker {
  private readonly donePromise: Promise<void>;

  constructor(
    private readonly pool: Pool,
    private readonly jobName: string,
    private readonly handler: WorkerHandler<Payload>,
    private readonly signal: AbortSignal,
    private readonly pollIntervalMs: number,
    private readonly ready: Promise<void>,
  ) {
    this.donePromise = this.run();
    // Swallow unhandled rejections — errors inside run() are surfaced via wait()
    this.donePromise.catch(() => undefined);
  }

  wait(): Promise<void> {
    return this.donePromise;
  }

  private async run(): Promise<void> {
    await this.ready;
    while (!this.signal.aborted) {
      const client = await this.pool.connect();
      let row: JobRow | null;
      try {
        row = await claimJob(client, this.jobName);
      } catch (err) {
        client.release(true);
        throw err;
      }
      client.release();

      if (!row) {
        await interruptibleSleep(this.pollIntervalMs, this.signal);
        continue;
      }

      const job = rowToJob<Payload>(row);
      try {
        await this.handler(job, this.signal);
        await completeJob(this.pool, job.id);
      } catch (err) {
        await failJob(this.pool, job.id, err);
      }
    }
  }
}
